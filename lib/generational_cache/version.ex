defmodule GenerationalCache.Version do
  @moduledoc """
  Behaviour for implementing versioning.

  For example, if you're inserting data from `Ecto` which uses optimistic
  locking,
  """

  @callback handle_update(GenerationalCache.data, integer) :: {:ok, GenerationalCache.data, integer} | :error
end