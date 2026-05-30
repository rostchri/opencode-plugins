# Spezifikation: Elixir Integration für das Ensemble Roleplay Plugin

Das neue OpenCode-Plugin (`chatroom-ensemble`) übernimmt das Prompt-Engineering und die Kommunikation mit der LLM-API. Der Elixir-basierte Chatroom-Controller muss daher seine interne `CharState` -> `Dispatcher` -> `OpenRouterClient` Kette aufgeben und die Turn-Ausführung an den Phoenix Channel delegieren.

Diese Spezifikation beschreibt die notwendigen Änderungen im Elixir-Code (`/container/chatroom/build/chatroom-controller/lib/chatroom_controller/ensemble/`).

---

## 1. Anpassung des `EnsembleRoom` und `CharState` (Delegation)

Aktuell baut der `CharState` den Prompt selbst (via `SystemPrompt.build_messages`) und startet einen asynchronen Task (via `Dispatcher.stream`). Dies muss ersetzt werden durch einen einfachen Broadcast an das Plugin.

### Alter Ablauf (`do_start_stream` in `CharState.ex`):
1. `SystemPrompt.build_messages` aufrufen
2. `Dispatcher.stream` aufrufen (startet Elixir-Task)
3. Callbacks (`on_token`, `on_done`) verarbeiten und per `broadcast` an Browser schicken

### Neuer Ablauf (`do_start_stream` in `CharState.ex`):
Der `CharState` fungiert nur noch als State-Halter. Er startet keinen eigenen Task mehr, sondern beauftragt das Plugin über PubSub.

```elixir
defp do_start_stream(stream_id, message_id, shared_history_window, is_continue, state) do
  # 1. Char Card aus dem State holen
  card = state.card

  # 2. History extrahieren (ohne interne System-Prompts, da das Plugin diese baut)
  history = shared_history_window
  
  # 3. Payload zusammenstellen
  payload = %{
    room_id: state.room_id,
    char_id: state.char_id,
    char_card: card,
    history: history,
    message_id: message_id,
    stream_id: stream_id,
    is_continue: is_continue
  }

  # 4. Broadcast an den Room-Channel.
  # Das OpenCode-Plugin lauscht auf diesem Topic.
  ChatroomController.Endpoint.broadcast!(
    "room:#{state.room_id}",
    "ensemble:run_turn",
    payload
  )

  # 5. State aktualisieren. Anstelle eines `task_pid` wird nur markiert,
  # dass wir auf den Plugin-Stream warten.
  stream = %{
    stream_id: stream_id,
    message_id: message_id,
    start_at: DateTime.utc_now(),
    buffer: []
  }

  %{state | active_stream: stream}
end
```

---

## 2. Rückkanal: Vom Plugin zum Chatroom Controller

Das OpenCode-Plugin schickt die generierten Tokens und das Abschluss-Signal via HTTP POST-Requests an die existierenden `StreamingController`-Endpunkte:

1. **Token Delta**: `POST /api/agent-stream-delta`
2. **Stream End**: `POST /api/agent-stream-complete`

### Anpassung im `StreamingController`
Der existierende `StreamingController` verarbeitet bereits diese Requests. Für das Ensemble-System müssen diese eingehenden HTTP-Requests jedoch auch dem `EnsembleRoom` / `CharState` gemeldet werden, damit dieser weiß, wann der Turn beendet ist (und den Lock `room_busy` wieder aufheben kann).

```elixir
# In ChatroomController.StreamingController

def stream_complete(conn, _opts) do
  # ... (bestehendes Body-Parsing)
  
  # Neu: EnsembleRoom über Stream-Abschluss informieren
  room_id = Map.get(body, "room_id")
  char_id = Map.get(body, "char_id") # (Vom Plugin im Body mitgeschickt)
  
  if room_type(room_id) == :ensemble do
    ChatroomController.Ensemble.EnsembleRoom.notify_stream_done(
      room_id, 
      char_id, 
      message_id, 
      full_text
    )
  end
  
  # ... (bestehendes ChatHistory.add und Broadcast an Browser)
end
```

---

## 3. `EnsembleRoom` Turn-Freigabe

Der `EnsembleRoom` (und der zugewiesene `CharState`) hält aktuell einen Lock auf den Raum (`active_stream`), solange ein Charakter spricht.

Wenn der `StreamingController` das `stream_complete` Event vom Plugin empfängt und via `notify_stream_done/4` an den `EnsembleRoom` weitergibt, muss folgendes passieren:

```elixir
# In EnsembleRoom.ex

def notify_stream_done(room_id, char_id, message_id, full_text) do
  GenServer.cast(via(room_id), {:stream_done, char_id, message_id, full_text})
end

def handle_cast({:stream_done, char_id, message_id, full_text}, state) do
  # 1. CharState den aktiven Stream löschen lassen
  case Map.get(state.chars, char_id) do
    pid when is_pid(pid) ->
      ChatroomController.Ensemble.CharState.mark_stream_done(pid)
    _ -> :ok
  end

  # 2. History im EnsembleRoom aktualisieren
  new_message = %{
    id: message_id,
    role: "assistant",
    name: get_char_name(state, char_id),
    content: full_text,
    char_id: char_id
  }
  
  new_history = state.shared_history ++ [new_message]
  
  # 3. TurnState freigeben (der nächste Char darf sprechen)
  new_turn_state = TurnState.clear_active_stream(state.turn_state, char_id)
  
  {:noreply, %{state | shared_history: new_history, turn_state: new_turn_state}}
end
```

---

## 4. Entfernung von Altlasten

Sobald das Plugin die Arbeit übernimmt, können folgende Module aus dem Elixir-Controller gelöscht oder stillgelegt werden, da sie nicht mehr benötigt werden:
- `ChatroomController.Ensemble.SystemPrompt` (Prompt-Bauen passiert im Plugin)
- `ChatroomController.Ensemble.Dispatcher` (Lokaler API-Client wird nicht mehr verwendet)
- `ChatroomController.Ensemble.OpenRouterClient` (Wird vom `@opencode-ai/sdk` im Plugin abgelöst)

## Fazit
Die Elixir-Integration ist ein reiner Architektur-Umbau von "Elixir-Pulls-API" hin zu "Elixir-Pushes-Event-To-Plugin". Der Controller teilt dem Plugin nur noch mit: *"Der Charakter mit dieser Char-Card ist jetzt dran, hier ist die History bis zu diesem Punkt."* Das Plugin baut den Token-genauen Prompt und füttert den Controller anschließend mit dem Ergebnis.
