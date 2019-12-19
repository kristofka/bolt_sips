defmodule Bolt.Sips.Internals.PackStream.V1Utils do
  alias Bolt.Sips.Internals.PackStream.Encoder

  defmacro __using__(_options) do
    quote do
      import unquote(__MODULE__)

      @spec encode_list_data(list(), integer()) :: [any()]
      defp encode_list_data(data, bolt_version) do
        Enum.map(
          data,
          &Encoder.encode(&1, bolt_version)
        )
      end

      @spec encode_kv(map(), integer()) :: binary()
      defp encode_kv(map, bolt_version) do
        Enum.reduce(map, <<>>, fn data, acc -> [acc, do_reduce_kv(data, bolt_version)] end)
      end

      @spec do_reduce_kv({atom(), any()}, integer()) :: [binary()]
      defp do_reduce_kv({key, value}, bolt_version) do
        [
          Encoder.encode(
            key,
            bolt_version
          ),
          Encoder.encode(value, bolt_version)
        ]
      end
    end
  end
end
