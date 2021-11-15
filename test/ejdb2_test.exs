  defmodule EJDB2Test do
  use ExUnit.Case, async: true
  doctest EJDB2

  require Logger

  @host "127.0.0.1"
  @coll "collection"

  setup %{test: name} do
    {:ok, bindport} = connect(name)

    Logger.warn "CONNECT ws://#{@host}:#{bindport}/"
    {:ok, pid} = EJDB2.Conn.start_link("ws://#{@host}:#{bindport}/")

    {:ok, %{
      conn: pid,
      port: bindport,
    }}
  end

  test "authentication token", opts do
    # Not implemented
    nil
  end


  test "set/get", %{conn: pid} do
    assert {:ok, %{"id" => 1, "test" => "a"}} == EJDB2.set pid, @coll, 1, %{test: "a"}
    assert {:ok, new = %{"id" => 1, "replace" => "all"}} == EJDB2.set pid, @coll, 1, %{replace: "all"}
    assert {:ok, ^new} = EJDB2.get pid, @coll, 1
  end


  test "add", %{conn: pid} do
    assert {:ok, %{"id" => 1, "value" => "a"}} == EJDB2.add pid, @coll, %{value: :a}
    assert {:ok, %{"id" => 2, "value" => "b"}} == EJDB2.add pid, @coll, %{value: :b}
    assert {:ok, %{"id" => 3, "value" => "c"}} == EJDB2.add pid, @coll, %{value: :c}
    assert {:ok, %{"id" => 4, "value" => "d"}} == EJDB2.add pid, @coll, %{value: :d}
    assert {:ok, %{"id" => 5, "value" => "e"}} == EJDB2.add pid, @coll, %{value: :e}
    assert {:ok, %{"id" => 6, "value" => "f"}} == EJDB2.add pid, @coll, %{value: :f}
  end


  test "delete document", %{conn: pid} do
    assert {:ok, a = %{"id" => 1, "value" => "a"}} == EJDB2.add pid, @coll, %{value: :a}
    assert {:ok, b = %{"id" => 2, "value" => "b"}} == EJDB2.add pid, @coll, %{value: :b}

    # Delete document by id
    assert {:ok, 1} == EJDB2.delete(pid, @coll, 1)
    assert {:error, :not_found} == EJDB2.delete(pid, @coll, 1)

    # Delete document
    assert {:ok, 2} == EJDB2.delete(pid, @coll, b)
    {:error, :not_found} == EJDB2.delete(pid, @coll, b)
    {:error, :not_found} == EJDB2.delete(pid, @coll, 2)
  end


  test "patch document", %{conn: pid} do
    assert {:ok, a = %{"id" => id = 1, "value" => "a"}} == EJDB2.add pid, @coll, %{value: :a}
    assert {:ok, Map.put(a, "additional", "data")} == EJDB2.patch(pid, @coll, id, %{additional: "data"})

    assert {:ok, Map.put(a, "additional", "next")} == EJDB2.patch(pid, @coll, id, additional: "next")
  end



  defp connect(name, additional \\ []) do
    bin = String.trim "#{:os.cmd('command -v jbs')}"
    bindport = 55000 + :rand.uniform(10000)
    file = "/tmp/ejdb-#{:erlang.phash2(name)}-#{:erlang.phash2(make_ref())}"
    args = [file: file, bind: @host, port: bindport] ++ additional
    args = for {k, v} <- args, reduce: [] do
      acc -> ["--#{k}", v | acc]
    end
    command = "#{bin} #{Enum.join(args, " ")}"

    Logger.error ("#{inspect self()} #{command}")


    {:ok, _, runner} = :exec.run(command, [:stdout, :stderr])
    receive do
      {:stderr, ^runner, line} ->
        true = String.match?(line, ~r/HTTP\/WS endpoint at #{@host}:#{bindport}\n$/)
      after 1000 ->
        Logger.error "Failed to start EJDB2 subprocess in time"
        exit(:timeout)
    end

    on_exit(fn ->
      :ok = :exec.stop(runner)
      receive do
        {:DOWN, _, :process, _, _} -> :ok
      after 100 ->
        :exec.kill(runner, :sigterm)
      end
      File.rm!(file)
    end)

    {:ok, bindport}
  end
end
