defmodule EJDB2.Conn do
  @moduledoc """
  Keep a websocket connection with EJDB2 instance
  """

  use WebSockex

  require Logger

  def start_link(opts \\ [])

  def start_link("" <> url) do
    start_link(uri: url)
  end

  def start_link(opts) do
    # If available use :name option such that it can be used with a Registry
    {genopts, opts} =
      case Keyword.pop(opts, :name) do
        {nil, opts} -> {[], opts}
        {name, opts} -> {[name: name], opts}
      end

    opts = Keyword.put_new(opts, :uri, "ws://127.0.0.1:9191")
    url = opts[:uri]

    WebSockex.start_link(url, __MODULE__, Enum.into(opts, %{}), genopts)
  end

  @doc """
  Send a command to the web socket process
  """
  def call(pid, [_ | _] = cmd, opts \\ []) do
    {timeout, opts} = Keyword.pop(opts, :timeout, 5000)
    multi? = true == opts[:multi]

    monref = Process.monitor(pid)
    reqid = Base.encode64(:binary.encode_unsigned(0x1234 + :rand.uniform(0xFFFFFF)))

    :ok = WebSockex.cast(pid, {:send, {self(), reqid}, cmd, opts})

    :ok =
      receive do
        {:DOWN, ^monref, :process, ^pid, _reason} ->
          Process.exit(self(), :noproc)
      after
        0 ->
          :ok
      end

    reply =
      receive do
        {:DOWN, ^monref, :process, ^pid, reason} ->
          Process.exit(self(), reason)

        {^reqid, :error, reason} ->
          {:error, reason}

        {^reqid, :reply, replies} when multi? ->
          json = for row <- replies, into: [], do: handle_document(row)
          {:ok, json}

        {^reqid, :reply, replies} when not multi? ->
          [reply] = replies
          json = handle_document(reply)
          {:ok, json}
      after
        timeout ->
          Process.exit(self(), :timeout)
      end

    Process.demonitor(monref)
    reply
  end

  defp handle_document(reply) do
    case String.split(reply, "\t", parts: 2) do
      [n, json] ->
        Map.put(Jason.decode!(json), "id", Jason.decode!(n))

      [n] ->
        # This is just an integer
        Jason.decode!(n)
    end
  end

  def handle_frame({:text, input}, state) do
    [reqid | msgs] = String.split(input, ~r/\s/, trim: true, parts: 2)
    data? = [] != msgs

    state =
      case state[reqid] do
        {pid, opts, acc} ->
          cond do
            data? and nil != str_to_error(hd(msgs)) ->
              error = str_to_error(hd(msgs))
              Logger.warn("[#{reqid}] << #{error}")
              send(pid, {reqid, :error, error})
              Map.drop(state, [reqid])

            true == opts[:multi] and data? ->
              nextstate = Map.put(state, reqid, {pid, opts, [hd(msgs) | acc]})
              nextstate

            # Final message, return the resultset
            true == opts[:multi] and [] == msgs ->
              send(pid, {reqid, :reply, acc})
              Map.drop(state, [reqid])

            true != opts[:multi] ->
              Logger.info("[#{reqid}] << #{inspect(msgs)}")
              send(pid, {reqid, :reply, msgs})
              Map.drop(state, [reqid])
          end

        nil ->
          state
      end

    {:ok, state}
  end

  def handle_cast({:send, {pid, reqid}, msg, opts}, state) do
    msg = Enum.join(msg, " ")
    Logger.info("[#{reqid}] >> #{msg}")
    {:reply, {:text, reqid <> " " <> msg}, Map.put(state, reqid, {pid, opts, []})}
  end

  defp str_to_error("ERROR: " <> _ = r) do
    case String.trim(String.trim(List.last(String.split(r)), ")"), "(") do
      "IWKV_ERROR_NOTFOUND" -> :not_found
    end
  end

  defp str_to_error(_r), do: nil
end
