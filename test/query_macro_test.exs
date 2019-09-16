defmodule QueryMacro.Test do
  use Bolt.Sips.ConnCase, async: true
  alias QueryMacro.Test
  alias Bolt.Sips.Test.Support.Database
  use Bolt.Sips.QueryMacro

  @cyp  """
    MATCH (n:Person {bolt_sips: true})
    RETURN n.name AS Name
    ORDER BY Name DESC
    LIMIT 5
  """

  @cyp2 "CREATE (n:Person)"

  defmodule TestUser do
    defstruct name: "", bolt_sips: true
  end

  defp rebuild_fixtures(conn) do
    Database.clear(conn)
    Bolt.Sips.Fixture.create_graph(conn, :bolt_sips)
  end

  setup(%{conn: conn} = context) do
    rebuild_fixtures(conn)
    {:ok, context}
  end

  test "a simple query that should work", context do
    conn = context[:conn]



     row = prequery_single conn,
                                 """
      MATCH (n:Person {bolt_sips: true})
      RETURN n.name AS Name
      ORDER BY Name DESC
      LIMIT 5
    """


    assert List.first(row)["Name"] == "Patrick Rothfuss",
           "missing 'The Name of the Wind' database, or data incomplete"
  end

end