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
  defmacro query(pid, collection, q \\ true, opts \\ []) do
    # prewalk, convert all compile time values to query string
    try do
      parts = compact(q)

      quote do
        parts = Enum.map(unquote(parts), fn {term} -> inspect(term); s -> s end)
        qstr = 
          case Enum.join(parts, " ") do
            "true" -> "*"
            qstr -> "[#{qstr}]"
          end

        qstr = "@#{unquote(collection)}/#{qstr}"
        outopts = Keyword.put(unquote(opts), :multi, true)
        Conn.call(unquote(pid), ["query", unquote(collection), qstr], outopts)
      end
    rescue e in ArgumentError ->
      {:error, e.message}
    end
  end

  @operators [:and, :or, :>, :<, :>=, :<=, :!=, :==, :in, :ni, :like]

  def strfn({:^, env, [e]}), do: {:^, env, [e]}
  def strfn(a), do: a

  # Quote ^bound variables in a tuple so we can easily detect them later
  def compact(term) when is_atom(term), do:    [Macro.to_string(term)]
  def compact(term) when is_float(term), do:   [Macro.to_string(term)]
  def compact(term) when is_integer(term), do: [Macro.to_string(term)]
  def compact(term) when is_binary(term), do:  [Macro.to_string(term)]
  def compact(term) when is_list(term) do
    compacted = for t <- term, do: compact(t)
    [
      "[",
      Enum.join(compacted, ", "),
      "]"
    ]
  end
  def compact({var, env, [{:like, env, [match]}]}) do
    regex =
      match
      |> String.replace("_", ".")
      |> String.replace("%", ".*?")
    [Macro.to_string({var, [], nil}), "re", inspect(regex)]
  end

  # Mark this for later
  def compact({:^, env, [e]}), do: {:{}, env, [e]}
  # Normal variables refers to a property in the model
  def compact({_var, _env, nil} = e), do: Macro.to_string(e)
  # Compact infix operators 
  def compact({op, env, [{_var, env, nil} = a, b]}) when op in @operators, do: [compact(a), "#{map_op(op)}", compact(b)]
  # Left hand side must be a property!!!
  def compact({op, _env, [a, _b]}), do: raise ArgumentError, message: "Left hand side of #{op} must be property; got #{Macro.to_string(a)}"
  # Rest can be used as-is
  def compact(a), do: a

  defp map_op(:==), do: "="
  defp map_op(op) when op in @operators, do: "#{op}"
end
