defmodule GenerationalCache.Version.Unversioned do
  @moduledoc false

  @behaviour GenerationalCache.Version

  def handle_update(new_data, _old_data, _old_version, _opts) do
    {:ok, {new_data, -1}}
  end
end