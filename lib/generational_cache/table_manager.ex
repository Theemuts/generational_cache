defmodule GenerationalCache.TableManager do
  @moduledoc """
  A `GenServer` which owns all the tables used by `GenerationalCache`. Each
  shard has at least two and at most three tables associated to it. The first
  is the hot cache, the second the cold cache, and the third is used when
  dropping the cold cache if the maximum cache size is exceeded.

  A lock table is also created, so we can block access to the shard when the
  cold cache is dropped.

  Currently, if this process crashes all tables are lost.
  """
  use GenServer

  @type cache_tables :: [atom]
  @type state :: cache_tables
  @type transfer :: {:"ETS-TRANSFER", pid, reference, []}
  @type init :: {:ok, state}
  @type info :: {:noreply, state}

  @doc false
  @spec start_link() :: GenServer.on_start
  def start_link() do
     GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @doc false
  @spec init(:ok) :: init
  def init(:ok) do
    shards = Application.get_env(:generational_cache, :shards, 2)

    tables = 0..shards-1
    |> Enum.reduce([], fn(shard, acc) ->
         {a, b, c} = GenerationalCache.Util.get_table_names(shard)
         [a, b, c | acc]
       end)

    tables
    |> Enum.chunk(2, 3)
    |> Enum.map(&Enum.map(&1, fn(t) -> :ets.new(t, [:named_table, :public, :set, read_concurrency: true, write_concurrency: true]) end))

    {:ok, tables}
  end

  # We get the table back during the cache drop process.
  @doc false
  @spec handle_info(transfer, state) :: info
  def handle_info({:"ETS-TRANSFER", _, _, []}, s) do
    {:noreply, s}
  end
end