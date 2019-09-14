defmodule Bolt.Sips.Internals.BoltProtocolAllBoltVersionTest do
  use ExUnit.Case, async: false
  alias Bolt.Sips.Internals.BoltProtocol

  alias Bolt.Sips.Internals.BoltProtocol
  setup do
    app_config = Application.get_env(:bolt_sips, Bolt)

    port = Keyword.get(app_config, :port, 7687)
    auth = {app_config[:basic_auth][:username], app_config[:basic_auth][:password]}

    config =
      app_config
      |> Keyword.put(:port, port)
      |> Keyword.put(:auth, auth)

    {:ok, port} =
      :gen_tcp.connect(config[:url], config[:port], active: false, mode: :binary, packet: :raw)

    {:ok, bolt_version} = BoltProtocol.handshake(:gen_tcp, port, [])
    {:ok, _} = BoltProtocol.hello(:gen_tcp, port, bolt_version, auth)

    on_exit(fn ->
      :gen_tcp.close(port)
    end)

    {:ok, config: config, port: port, bolt_version: bolt_version}
  end
  test "works for small queries", %{port: port, bolt_version: bolt_version} do
    string = Enum.to_list(0..100) |> Enum.join()

    query = """
      RETURN {string} as string
    """

    params = %{string: string}

    [{:success, _} | records] =
      BoltProtocol.run_statement(:gen_tcp, port, bolt_version, query, params)

    assert [record: [^string], success: _] = records
  end

  test "works for big queries", %{port: port, bolt_version: bolt_version} do
    string = Enum.to_list(0..25_000) |> Enum.join()

    query = """
      RETURN {string} as string
    """

    params = %{string: string}

    [{:success, _} | records] =
      BoltProtocol.run_statement(:gen_tcp, port, bolt_version, query, params)
    assert [record: [^string], success: _] = records
  end

  test "returns errors for wrong cypher queris", %{port: port, bolt_version: bolt_version} do
    assert %Bolt.Sips.Internals.Error{type: :cypher_error} =
             BoltProtocol.run_statement(:gen_tcp, port, bolt_version, "What?")
  end

  test "allows to recover from error with reset", %{port: port, bolt_version: bolt_version} do
    assert %Bolt.Sips.Internals.Error{type: :cypher_error} =
             BoltProtocol.run_statement(:gen_tcp, port, bolt_version, "What?")

    assert :ok = BoltProtocol.reset(:gen_tcp, port, bolt_version)

    assert [{:success, _} | _] =
             BoltProtocol.run_statement(:gen_tcp, port, bolt_version, "RETURN 1 as num")
  end

  test "returns proper error when using a bad session", %{port: port, bolt_version: bolt_version} do
    assert %Bolt.Sips.Internals.Error{type: :cypher_error} =
             BoltProtocol.run_statement(:gen_tcp, port, bolt_version, "What?")

    error = BoltProtocol.run_statement(:gen_tcp, port, bolt_version, "RETURN 1 as num")

    assert %Bolt.Sips.Internals.Error{} = error
    assert error.message =~ ~r/The session is in a failed state/
  end

  test "returns proper error when misusing reset", %{
    port: port,
    bolt_version: bolt_version
  } do
    :gen_tcp.close(port)
    assert %Bolt.Sips.Internals.Error{} = BoltProtocol.reset(:gen_tcp, port, bolt_version)
  end

  test "returns proper error when using a closed port", %{port: port, bolt_version: bolt_version} do
    :gen_tcp.close(port)

    assert %Bolt.Sips.Internals.Error{type: :connection_error} =
             BoltProtocol.run_statement(:gen_tcp, port, bolt_version, "RETURN 1 as num")
  end

  test "an invalid parameter value yields an error", %{port: port, bolt_version: bolt_version} do
    cypher = "MATCH (n:Person {invalid: {a_tuple}}) RETURN TRUE"

    assert_raise Bolt.Sips.Internals.PackStreamError, ~r/^unable to encode/i, fn ->
      BoltProtocol.run_statement(:gen_tcp, port, bolt_version, cypher, %{
        a_tuple: {:error, "don't work"}
      })
    end
  end
end
