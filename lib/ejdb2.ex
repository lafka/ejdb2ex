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
  def set(pid, collection, id, body, opts) when is_integer(id)  do
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
  def query(pid, collection, query, opts \\ []) do
    Conn.call(pid, ["queyr", collection, query], opts)
  end

end
