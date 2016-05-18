defmodule GenerationalCache.Shard.PoolsSupervisor do
  @moduledoc false

  use Supervisor

  @doc false
  @spec start_link() :: GenServer.on_start
  def start_link() do
    Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @doc false
  @spec init(:ok) :: {:ok, tuple}
  def init(:ok) do
    shards = Application.get_env(:generational_cache, :shards, 2)

    0..shards-1
    |> Enum.map(&supervisor(GenerationalCache.Shard.Pool, [&1], id: &1))
    |> supervise(strategy: :one_for_one)
  end
end