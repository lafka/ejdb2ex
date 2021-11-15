defmodule EJDB2.Conn do
  use WebSockex

  require Logger

  def start_link(opts \\ [])
  def start_link("" <> url) do
    start_link([uri: url])
  end
  def start_link(opts) do
    opts = Keyword.put_new(opts, :uri, "ws://127.0.0.1:9191")
    url = opts[:uri]
    WebSockex.start_link(url, __MODULE__, Enum.into(opts, %{}))
  end

  @doc """
  Send a command to the web socket process
  """
  def call(pid, [_|_] = cmd, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 5000)

    monref = Process.monitor(pid)
    reqid = Base.encode64(:binary.encode_unsigned(0x1234 + :rand.uniform(0xffffff)))

    :ok = WebSockex.cast(pid, {:send, {self(), reqid}, cmd})


    :ok =
      receive do
        {:DOWN, ^monref, :process, ^pid, _reason} ->
          Process.exit(self(), :noproc)
      after 0 ->
        :ok
      end

    reply =
      receive do
        {:DOWN, ^monref, :process, ^pid, reason} ->
          Process.exit(self(), reason)

        {^reqid, :error, reason} ->
          {:error, reason}

        {^reqid, :reply, reply} ->
          json =
            case String.split(reply, "\t", parts: 2) do
              [n, json] ->
                Map.put(Jason.decode!(json), "id", Jason.decode!(n))

              [n] ->
                # This is just an integer
                Jason.decode!(n)
            end
          {:ok, json}

      after timeout ->
        Process.exit(self(), :timeout)
      end

      Process.demonitor(monref)
      reply
  end


  def handle_frame({:text, msg}, state) do
    state =
      case String.split(msg, ~r/\s/, trim: true, parts: 2) do
        [reqid, msg] ->
          error = str_to_error(msg)
          case state[reqid] do
            pid when is_pid(pid) ->
              if error do
                Logger.warn "[#{reqid}] << #{error}"
                send pid, {reqid, :error, error}
              else
                Logger.info "[#{reqid}] << #{msg}"
                send pid, {reqid, :reply, msg}
              end
              Map.drop(state, [reqid])

            nil ->
              state
          end

        _ ->
          Logger.warn "[???] << #{msg}"
          state
      end


    {:ok, state}
  end

  def handle_cast({:send, {pid, reqid}, msg}, state) do
    msg = Enum.join(msg, " ")
    Logger.info "[#{reqid}] >> #{msg}"
    {:reply, {:text, reqid <> " " <> msg}, Map.put(state, reqid, pid)}
  end

  defp str_to_error("ERROR: " <> _ = r) do
    case String.trim(String.trim(List.last(String.split(r)), ")"), "(") do
      "IWKV_ERROR_NOTFOUND" -> :not_found
    end
  end
  defp str_to_error(_r), do: nil
end
