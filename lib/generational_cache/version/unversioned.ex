defmodule GenerationalCache.Version.Unversioned do
  @moduledoc """
  Default version handler, never blocks insert, but always allows overwriting
  the (possibly) existing entry.
  """

  @behaviour GenerationalCache.Version

  @doc false
  def handle_insert(new_data, _old_data \\ nil, _old_version \\ nil, _opts \\ nil) do
    {:ok, {new_data, -1}}
  end
end