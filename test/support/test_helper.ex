defmodule GenerationalCache.Support.TestHelper do
  @moduledoc false

  alias GenerationalCache.CacheDropServer

  def do_setup do
    # Make sure all deletes are complete
    # Drop twice to get rid of all data.
    :timer.sleep(250)
    CacheDropServer.drop_cold_cache
    :timer.sleep(250)
    CacheDropServer.drop_cold_cache
    :timer.sleep(250)
  end
end