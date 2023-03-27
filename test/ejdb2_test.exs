defmodule EJDB2Test do
  use ExUnit.Case, async: true
  doctest EJDB2

  require Logger
  require EJDB2

  @host "127.0.0.1"
  @coll "collection"

  setup %{test: name} do
    {:ok, pid} = connect(name)

    {:ok,
     %{
       conn: pid
     }}
  end

  test "authentication token", _opts do
    # Not implemented
    nil
  end

  test "set/get", %{conn: pid} do
    assert {:ok, %{"id" => 1, "test" => "a"}} == EJDB2.set(pid, @coll, 1, %{test: "a"})

    assert {:ok, new = %{"id" => 1, "replace" => "all"}} ==
             EJDB2.set(pid, @coll, 1, %{replace: "all"})

    assert {:ok, {_id, ^new}} = EJDB2.get(pid, @coll, 1)
  end

  test "add", %{conn: pid} do
    assert {:ok, %{"id" => 1, "value" => "a"}} == EJDB2.add(pid, @coll, %{"value" => "a"})
    assert {:ok, %{"id" => 2, "value" => "b"}} == EJDB2.add(pid, @coll, %{"value" => "b"})
    assert {:ok, %{"id" => 3, "value" => "c"}} == EJDB2.add(pid, @coll, %{"value" => "c"})
    assert {:ok, %{"id" => 4, "value" => "d"}} == EJDB2.add(pid, @coll, %{"value" => "d"})
    assert {:ok, %{"id" => 5, "value" => "e"}} == EJDB2.add(pid, @coll, %{"value" => "e"})
    assert {:ok, %{"id" => 6, "value" => "f"}} == EJDB2.add(pid, @coll, %{"value" => "f"})
  end

  test "delete document by id", %{conn: pid} do
    assert {:ok, _a = %{"id" => 1, "value" => "a"}} == EJDB2.add(pid, @coll, %{"value" => "a"})
    assert {:ok, b = %{"id" => 2, "value" => "b"}} == EJDB2.add(pid, @coll, %{"value" => "b"})

    # Delete document by id
    assert {:ok, 1} == EJDB2.delete(pid, @coll, 1)
    assert {:error, :not_found} == EJDB2.delete(pid, @coll, 1)

    # Delete document
    assert {:ok, 2} == EJDB2.delete(pid, @coll, b)
    assert {:error, :not_found} == EJDB2.delete(pid, @coll, b)
    assert {:error, :not_found} == EJDB2.delete(pid, @coll, 2)
  end

  test "patch document", %{conn: pid} do
    assert {:ok, a = %{"id" => id = 1, "value" => "a"}} ==
             EJDB2.add(pid, @coll, %{"value" => "a"})

    assert {:ok, {id, Map.put(a, "additional", "data")}} ==
             EJDB2.patch(pid, @coll, id, %{"additional" => "data"})

    assert {:ok, {id, Map.put(a, "additional", "next")}} ==
             EJDB2.patch(pid, @coll, id, %{"additional" => "next"})
  end

  test "query", %{conn: pid} do
    objects = objects(pid, 6)

    assert {:ok, rows} = EJDB2.query(pid, EJDB2.from(@coll))

    assert rows == [
             %{"id" => 1, "value" => 1},
             %{"id" => 2, "value" => 2},
             %{"id" => 3, "value" => 3},
             %{"id" => 4, "value" => 4},
             %{"id" => 5, "value" => 5},
             %{"id" => 6, "value" => 6},
             %{"id" => 7, "value" => 7}
           ]

    assert rows == objects
  end

  test "query: basic operators", %{conn: pid} do
    all = objects(pid, 5)

    # equal to
    assert {:ok, rows} = EJDB2.query(pid, EJDB2.from(@coll, value == 4))
    assert rows == [%{"id" => 4, "value" => 4}]

    # not equal to
    assert {:ok, rows} = EJDB2.query(pid, EJDB2.from(@coll, value != 4))
    assert rows == all -- [%{"id" => 4, "value" => 4}]

    # greater than
    assert {:ok, rows} = EJDB2.query(pid, EJDB2.from(@coll, value > 4))
    assert rows == [%{"id" => 5, "value" => 5}, %{"id" => 6, "value" => 6}]

    # greater than or equals to
    assert {:ok, rows} = EJDB2.query(pid, EJDB2.from(@coll, value >= 5))
    assert rows == [%{"id" => 5, "value" => 5}, %{"id" => 6, "value" => 6}]

    # less than
    assert {:ok, rows} = EJDB2.query(pid, EJDB2.from(@coll, value < 3))
    assert rows == [%{"id" => 1, "value" => 1}, %{"id" => 2, "value" => 2}]

    # less than or equals to
    assert {:ok, rows} = EJDB2.query(pid, EJDB2.from(@coll, value <= 2))
    assert rows == [%{"id" => 1, "value" => 1}, %{"id" => 2, "value" => 2}]

    # value in set
    assert {:ok, rows} = EJDB2.query(pid, EJDB2.from(@coll, value in [2, 3]))
    assert rows == [%{"id" => 2, "value" => 2}, %{"id" => 3, "value" => 3}]

    # # value not in set - not working currently
    # assert {:ok, rows} = EJDB2.query(pid, @coll, value not in [4])
    # assert rows == [%{"id" => 2, "value" => 2}, %{"id" => 3, "value" => 3}]
  end

  test "query: and operator", %{conn: pid} do
    objects(pid, 5)

    assert {:ok, rows} = EJDB2.query(pid, EJDB2.from(@coll, value > 3 and value < 5))
    assert rows == [%{"id" => 4, "value" => 4}]

    {a, b} = {3, 5}
    assert {:ok, rows} = EJDB2.query(pid, EJDB2.from(@coll, value > ^a and value < ^b))
    assert rows == [%{"id" => 4, "value" => 4}]
  end

  test "query: or operator", %{conn: pid} do
    objects(pid, 5)

    {a, b} = {3, 5}
    assert {:ok, rows} = EJDB2.query(pid, EJDB2.from(@coll, value == 3 or value == 5))
    assert rows == [%{"id" => 3, "value" => 3}, %{"id" => 5, "value" => 5}]

    assert {:ok, rows} = EJDB2.query(pid, EJDB2.from(@coll, value == ^a or value == ^b))
    assert rows == [%{"id" => 3, "value" => 3}, %{"id" => 5, "value" => 5}]
  end

  test "query: dot expansion for bound variable", %{conn: pid} do
    objects(pid, 5)

    data = %{a: 1}
    assert {:ok, rows} = EJDB2.query(pid, EJDB2.from(@coll, value == ^data.a))
    assert rows == [%{"id" => 1, "value" => 1}]
  end

  test "query: regex", %{conn: pid} do
    {:ok, a} = EJDB2.add(pid, @coll, %{"value" => "some sample"})
    {:ok, b} = EJDB2.add(pid, @coll, %{"value" => "other sample"})
    {:ok, c} = EJDB2.add(pid, @coll, %{"value" => "sample later"})
    {:ok, d} = EJDB2.add(pid, @coll, %{"value" => "unsampled"})
    {:ok, _e} = EJDB2.add(pid, @coll, %{"value" => "other"})

    assert {:ok, [c]} == EJDB2.query(pid, EJDB2.from(@coll, value(like("sample%"))))
    assert {:ok, [a, b, c, d]} == EJDB2.query(pid, EJDB2.from(@coll, value(like("%sample%"))))
    assert {:ok, []} == EJDB2.query(pid, EJDB2.from(@coll, value(like("%not included%"))))


    assert {:ok, [c]} == EJDB2.query(pid, EJDB2.from(@coll, re(value, "^sample")))
    assert {:ok, [a, b, c, d]} == EJDB2.query(pid, EJDB2.from(@coll, re(value, ".*sample.*")))
    assert {:ok, []} == EJDB2.query(pid, EJDB2.from(@coll, re(value, "%not included%")))

    # There's no support for start and end of line matches so we can't make
    # an exact copy of (NOT) LIKE
    # assert {:ok, [e]} == EJDB2.query(pid, @coll, value like "other")
    # assert {:ok, [a, b]} == EJDB2.query(pid, @coll, value like "%sample")
  end

  test "query: bind value", %{conn: pid} do
    {:ok, a} = EJDB2.add(pid, @coll, %{"value" => "some sample"})
    {:ok, %{"value" => val} = b} = EJDB2.add(pid, @coll, %{"value" => "unsampled"})

    assert {:ok, [a]} == EJDB2.query(pid, EJDB2.from(@coll, value != ^val))
    assert {:ok, [b]} == EJDB2.query(pid, EJDB2.from(@coll, value == ^val))
  end

  test "query: path selection", %{conn: pid} do
    selected = Enum.filter(objects(pid, 5, true), fn %{"value" => v} -> v == 3 end)

    data = 3
    assert {:ok, rows} = EJDB2.query(pid, EJDB2.from(@coll, nested.value == ^data))
    assert rows == selected

    data = %{a: 3}
    assert {:ok, rows} = EJDB2.query(pid, EJDB2.from(@coll, nested.value == ^data.a))
    assert rows == selected
  end

  test "query: wildcard path", %{conn: pid} do
    selected = Enum.filter(objects(pid, 5, true), fn %{"value" => v} -> v == 3 end)

    data = 3
    assert {:ok, rows} = EJDB2.query(pid, EJDB2.from(@coll, _.value == ^data))
    assert rows == selected

    data = %{a: 3}
    assert {:ok, rows} = EJDB2.query(pid, EJDB2.from(@coll, deep.__.value == ^data.a))
    assert rows == selected
  end

  test "info", %{conn: pid} do
    assert {:ok, %{"id" => 1, "value" => "a"}} == EJDB2.add(pid, @coll, %{"value" => "a"})
    {:ok, a} = EJDB2.info(pid)

    assert %{
             "collections" => [
               %{"dbid" => 3, "indexes" => [], "name" => "collection", "rnum" => 1}
             ],
             "file" => _,
             "size" => _,
             "version" => _
           } = a
  end

  test "add index", %{conn: pid} do
    {:ok, %{"collections" => []}} = EJDB2.info(pid)
    assert :ok == EJDB2.idx(pid, @coll, 5, "/id")
  end

  defp objects(pid, n, nested \\ false) when n > 0 do
    for r <- 1..(1 + n) do
      value =
        if nested do
          %{"value" => r, "nested" => %{"value" => r}, "deep" => %{"deep" => %{"value" => r}}}
        else
          %{"value" => r}
        end

      {:ok, obj} = EJDB2.add(pid, @coll, value)
      obj
    end
  end

  defp connect(name, additional \\ []) do
    {bin, 0} = System.cmd("which", ["jbs"])
    bin = String.trim(bin)
    bindport = 55_000 + :rand.uniform(10_000)
    file = "/tmp/ejdb-#{:erlang.phash2(name)}-#{:erlang.phash2(make_ref())}"
    args = [file: file, bind: @host, port: bindport] ++ additional

    args =
      for {k, v} <- args, reduce: [] do
        acc -> ["--#{k}", v | acc]
      end

    command = "#{bin} #{Enum.join(["--trylock", "--trunc" | args], " ")}"

    Logger.debug("#{inspect(self())} exec - #{command}")

    {:ok, _, runner} = :exec.run(command, [:stdout, :stderr])

    await_startup = fn fun, acc ->
      case EJDB2.Conn.start_link("ws://#{@host}:#{bindport}/") do
        {:ok, pid} ->
          {:ok, pid}

        _ when acc < 10 ->
          Process.sleep(100)
          fun.(fun, acc + 1)

        _ ->
          Logger.error("Failed to start EJDB2 subprocess in 10 attempts")
          exit(:timeout)
      end
    end

    Logger.warn("TRYING TO CONNECT ws://#{@host}:#{bindport}/")

    {:ok, pid} = await_startup.(await_startup, 1)

    on_exit(fn ->
      :ok = :exec.stop(runner)

      receive do
        {:DOWN, _, :process, _, _} -> :ok
      after
        100 ->
          :exec.kill(runner, :sigterm)
      end

      File.rm!(file)
    end)

    {:ok, pid}
  end
end
