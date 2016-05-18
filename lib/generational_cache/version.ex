defmodule GenerationalCache.Version do
  @moduledoc """
  Behaviour for implementing versioning. You must implement one function,
  `:handle_insert/4`. Its parameters are:

    - `new_data`: the newly inserted data.
    - `current_data`: the data which is currently cached.
    - `current_version`: the version of the data which is currently cached.
    - `opts`: options, can be set by setting the third parameter in
    `GenerationalCache.insert/4` to `{MyVersion, opts}`. Defaults to an empty
    list if only the module is given as third parameter.

  This function should return either `:error` if the new data is stale and
  must not be inserted, or `{:ok, {new_data, new_version}}` when the new data
  is more recent than the cached version.
  """

  @callback handle_insert(GenerationalCache.data, GenerationalCache.data, integer, any) :: {:ok, {GenerationalCache.data, integer}} | :error
end