defmodule GenerationalCache.Version do
  @moduledoc """
  Behaviour for implementing versioning.

  For example, if you're inserting data from `Ecto` which uses optimistic
  locking,
  """

  @callback handle_insert(GenerationalCache.data, any) :: :ok | {:error, GenerationalCache.version}
  @callback handle_update(GenerationalCache.data, GenerationalCache.data, integer, any) :: {:ok, GenerationalCache.data, integer} | :error
end