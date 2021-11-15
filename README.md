# EJDB2

Integration with [ejdb](https://github.com/Softmotions/ejdb) over the websocket API.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `ejdb2` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:ejdb2, "~> 0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at [https://hexdocs.pm/ejdb2](https://hexdocs.pm/ejdb2).


## Usage

```elixir
import EJDB
{:ok, db} = start_link("ws://127.0.0.1:9191")

{:ok, mike} = add(:resource, %{key: "xLhfWtIC",
                               name: "mike",
                               email: "maki@example.com",
                               knows: ["1+RRTytj", "UFg6oFMV"]})

{:ok, jane} = add(:resource, %{key: "1+RRTytj",
                               name: "jane",
                               email: "jane@example.com",
                               knows: ["xLHfWtIC"]})

{:ok, vijay} = add(:resource, %{key: "UFg6oFMV",
                                name: "VJ",
                                email: "vj@example.com",
                                knows: ["xLHfWtIC"]})

now = DateTime.utc_now()
# Replace whole vijay
{:ok, vijay} = set(:resource, vijay["id"], Map.put(vijay, :seen, now))

# Update a single field to jane
{:ok, jane} = patch(:resource, jane, seen: DateTime.add(now, -86400))
```
