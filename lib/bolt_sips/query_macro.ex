defmodule Bolt.Sips.QueryMacro do
  @moduledoc false
  alias Bolt.Sips
  alias Bolt.Sips.{QueryStatement, Response, Types, Exception}
  defmacro __using__(_opts) do
    quote do
      import unquote(__MODULE__)
    end
  end

  defmacro prequery_single(conn, statement) do

    cpiled = Bolt.Sips.Internals.PackStream.MainEncoder.encode_string(statement,3)
    quer = %Bolt.Sips.QueryStatement{statement: cpiled}
    quote do
      exec = fn conn ->

        case DBConnection.execute(conn, unquote(Macro.escape(quer)), %{}) do
          {:ok, _query, resp} -> resp
          other -> other
        end
      end
      Response.transform(DBConnection.run(unquote(conn), exec,  [pool: Sips.config(:pool)]))
    end
  end

end
