defmodule GenerationalCache.App do
  @moduledoc false
  use Application

  def start(_type, _args) do
    import Supervisor.Spec, warn: false

    IO.puts "start app"

    children = [
      worker(GenerationalCache.TableManager, []),
      worker(GenerationalCache.CacheDropServer, []),
      supervisor(GenerationalCache.Shard.PoolsSupervisor, []),
      worker(Task, [&GenerationalCache.Task.SizeMonitor.run/0])
    ]

    opts = [strategy: :one_for_one, name: GenerationalCache.Supervisor]
    Supervisor.start_link(children, opts)
  end
end