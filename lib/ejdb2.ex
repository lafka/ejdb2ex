defmodule EJDB2 do
  @moduledoc """
  Documentation for `EJDB2`.
  """

  alias EJDB2.Conn

  @doc """
  Retrieve information about the database
  """
  def info(pid, opts \\ []), do: Conn.call(pid, ["info"], opts)

  def get(pid, collection, id, opts \\ []) do
    Conn.call(pid, ["get", collection, id], opts)
  end

  @doc """
  Replace or create document identified by `id` in `collection`
  """
  def set(pid, collection, id, body, opts \\ [])

  def set(pid, collection, id, body, opts) when is_integer(id) do
    encoded = Jason.encode!(Enum.into(body, %{"id" => id}))

    with {:ok, recvid} <- Conn.call(pid, ["set", collection, id, encoded], opts) do
      ^recvid = id
      {:ok, Jason.decode!(encoded)}
    end
  end

  @doc """
  Add a new document to collection without specifying a primary key
  """
  def add(pid, collection, body, opts \\ []) do
    encoded = Jason.encode!(body)

    with {:ok, id} <- Conn.call(pid, ["add", collection, encoded], opts) do
      {:ok, Map.put(Jason.decode!(encoded), "id", id)}
    end
  end

  @doc """
  Delete document :id in :collection
  """
  def delete(pid, collection, id, opts \\ [])
  def delete(pid, collection, %{"id" => id}, opts), do: delete(pid, collection, id, opts)

  def delete(pid, collection, id, opts) when is_integer(id) do
    Conn.call(pid, ["del", collection, id], opts)
  end

  @doc """
  Patch the given document
  """
  def patch(pid, collection, old, patch, opts \\ [])

  def patch(pid, collection, %{"id" => id}, patch, opts) do
    patch(pid, collection, id, patch, opts)
  end

  def patch(pid, collection, id, patch, opts) do
    encoded = Jason.encode!(Enum.into(patch, %{"id" => id}))

    with {:ok, ^id} <- Conn.call(pid, ["patch", collection, id, encoded], opts) do
      get(pid, collection, id)
    end
  end

  @doc """
  Add a index of type :mode on :path in :collection
  """
  def idx(pid, collection, mode, path, opts \\ []) do
    Conn.call(pid, ["idx", collection, mode, path], opts)
  end

  @doc """
  Remove the index on :path from :colletion
  """
  def rmi(pid, collection, mode, path, opts \\ []) do
    Conn.call(pid, ["rmi", collection, mode, path], opts)
  end

  @doc """
  Remove entire collection
  """
  def rmc(pid, collection, opts \\ []) do
    Conn.call(pid, ["rmc", collection], opts)
  end

  @doc """
  Perform a query
  """
  defmacro query(pid, collection, query \\ true, opts \\ []) do
    with {:ok, q2} <- transform(query) do
      qstr =
        case Macro.to_string(q2, &strfn/2) do
          "true" -> "*"
          qstr -> "[#{qstr}]"
        end

      quote do
        qstr = "@#{unquote(collection)}/#{unquote(qstr)}"
        outopts = Keyword.put(unquote(opts), :multi, true)
        Conn.call(unquote(pid), ["query", unquote(collection), qstr], outopts)
      end
    end
  end

  # defp strfn({:!, _env, [{:in, _env2, [a, b]}]}, _oldstr) do
  #   "#{Macro.to_string(a, &strfn/2)} ni #{Macro.to_string(b, &strfn/2)}"
  # end
  defp strfn({:!, _env, [{:in, _env2, [a, b]}]}, _oldstr) do
    "#{Macro.to_string(a, &strfn/2)} in #{Macro.to_string(b, &strfn/2)}"
  end

  defp strfn({:like, _env, [source, match]}, _oldstr) do
    regex =
      match
      |> String.replace("_", ".")
      |> String.replace("%", ".*?")

    "#{source} re \"^#{regex}$\""
  end

  defp strfn(_token, string) do
    string
  end

  # defp validate({:and, _, [a, b]}), do: validate(a) and validate(b)
  @operators [:and, :or, :>, :<, :>=, :<=, :!=, :==, :in, :ni, :like]
  # defp transform({:not, env, [{:in, _env2, [a, b]}]}), do: transform({:ni, env, [a, b]})
  defp transform({op, env, [a, b]}) when op in @operators do
    with {:ok, a} <- transform(a),
         {:ok, b} <- transform(b) do
      {:ok, translate({op, env, [a, b]})}
    end
  end

  # like is not an infix operator
  defp transform({a, aenv, [{:like, _env, [b]}]}), do: transform({:like, aenv, [a, b]})
  # normalize all negation to use !
  defp transform({op, env, [a]}) when op in [:not, :!], do: {:ok, {:!, env, [a]}}
  # we don't support nesting as of now
  defp transform({{:., _, _}, _, _}), do: {:error, :nesting}
  defp transform({:., _, _}), do: {:error, :nesting}
  # Keep variables
  defp transform({var, env, nil}), do: {:ok, {var, env, nil}}
  # Keep distinct values
  defp transform(n) when is_integer(n) or is_float(n), do: {:ok, n}
  defp transform(s) when is_binary(s), do: {:ok, s}
  defp transform(a) when is_atom(a), do: {:ok, a}
  defp transform(l) when is_list(l), do: {:ok, l}

  defp translate({:==, env, args}), do: {:=, env, args}
  defp translate({op, env, args}), do: {op, env, args}
end
