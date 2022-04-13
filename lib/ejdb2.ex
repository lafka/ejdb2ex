defmodule EJDB2 do
  @moduledoc """
  Documentation for `EJDB2`.
  """

  alias EJDB2.Conn

  @doc """
  Retrieve information about the database
  """
  def info(pid, opts \\ []) do
    with {:ok, {res, nil}} <- Conn.call(pid, ["info"], opts), do: {:ok, res}
  end

  def get(pid, collection, id, opts \\ []) do
    idfield = opts[:id] || "id"
    with {^id, data} <- Conn.call(pid, ["get", collection, id], opts) do
      body = Map.put(data, idfield, id)
      {:ok, Jason.encode!(body)}
    end
  end

  @doc """
  Replace or create document identified by `id` in `collection`
  """
  def set(pid, collection, id, body, opts \\ [])

  def set(pid, collection, id, body, opts) when is_integer(id) do
    idfield = opts[:id] || "id"
    body = Map.put(body, idfield, id)
    encoded = Jason.encode!(body)

    with {:ok, {^id, nil}} <- Conn.call(pid, ["set", collection, id, encoded], opts) do
      {:ok, Jason.decode!(encoded)}
    end
  end

  @doc """
  Add a new document to collection without specifying a primary key
  """
  def add(pid, collection, body, opts \\ []) do
    idfield = opts[:id] || "id"
    encoded = Jason.encode!(body)

    with {:ok, {id, nil}} <- Conn.call(pid, ["add", collection, encoded], opts) do
      body = Map.put(body, idfield, id)

      {:ok, body}
    end
  end

  @doc """
  Delete document :id in :collection
  """
  def delete(pid, collection, id, opts \\ [])
  def delete(pid, collection, %{} = fields, opts) do
    idfield = opts[:id] || "id"
    {:ok, id} = Map.fetch(fields, idfield)
    delete(pid, collection, id, opts)
  end

  def delete(pid, collection, id, opts) when is_integer(id) do
    with {:ok, res} <- Conn.call(pid, ["del", collection, id], opts) do
      {^id, nil} = res
      {:ok, id}
    end
  end

  @doc """
  Patch the given document
  """
  def patch(pid, collection, old, patch, opts \\ [])

  def patch(pid, collection, %{} = fields, patch, opts) do
    idfield = opts[:id] || "id"
    {:ok, id} = Map.fetch(fields, idfield)
    patch(pid, collection, id, patch, opts)
  end

  def patch(pid, collection, id, patch, opts) do
    idfield = opts[:id] || "id"

    patch = Map.put(patch, idfield, id)
    encoded = Jason.encode!(patch)

    with {:ok, {^id, nil}} <- Conn.call(pid, ["patch", collection, id, encoded], opts) do
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
  def query(pid, {collection, qstr}, opts \\ []) do
    idfield = opts[:id] || "id"
    # prewalk, convert all compile time values to query string
    outopts = Keyword.put(opts, :multi, true)
    res = Conn.call(pid, ["query", collection, qstr], outopts)
    with {:ok, rows} <- res do
      rows = for {id, data} <- rows, do: Map.put(data, idfield, id)
      {:ok, rows}
    end
  end


  @doc """
  Build a query string
  """
  defmacro from(collection, q \\ true) do
    try do
      parts = compact(q)

      quote do
        strparts = unquote(parts)
            |> List.flatten()
            |> Enum.map(fn {term} -> inspect(term); s -> s end)

        qstr =
          case Enum.join(strparts, " ") do
            "true" -> "*"
            qstr -> "[#{qstr}]"
          end

        {unquote(collection), "@#{unquote(collection)}/#{qstr}"}
      end
    rescue e in ArgumentError ->
      {:error, e.message}
    end
  end


  @operators [:>, :<, :>=, :<=, :!=, :==, :in, :ni, :like]
  @logical_ops [:and, :or]

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
  def compact({op, _env, [a, b]}) when op in @logical_ops, do: [compact(a), "#{map_op(op)}", compact(b)]
  # Left hand side must be a property!!!
  def compact({op, _env, [a, _b]}) when op not in @logical_ops, do: raise ArgumentError, message: "Left hand side of #{op} must be property; got #{Macro.to_string(a)}"
  # Rest can be used as-is
  def compact(a), do: a

  defp map_op(:==), do: "="
  defp map_op(op), do: "#{op}"
end
