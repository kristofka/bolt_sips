defmodule Rety.Backoff.Test do
  use ExUnit.Case, async: true
  import Stream

  use Retry

  setup_all do
    {:ok, [conn: Bolt.Sips.conn()]}
  end

  test "retry retries execution for specified attempts using an invalid Cypher command",
       context do
    conn = context[:conn]

    {elapsed, _} =
      :timer.tc(fn ->
        {:error, %Bolt.Sips.Error{message: message}} =
          retry with: lin_backoff(500, 1) |> take(5) do
            Bolt.Sips.query(conn, "INVALID CYPHER")
          end

        assert message =~ "INVALID CYPHER"
      end)

    assert elapsed / 1000 >= 2500
  end
end
