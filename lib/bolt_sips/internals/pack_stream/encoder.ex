alias Bolt.Sips.Internals.PackStream
alias Bolt.Sips.Internals.PackStream.EncoderHelper
alias Bolt.Sips.Internals.PackStream.MainEncoder

defprotocol Bolt.Sips.Internals.PackStream.Encoder do
  @moduledoc false

  # Encodes an item to its binary PackStream Representation
  #
  # Implementation exists for following types:
  #   - Integer
  #   - Float
  #   - List
  #   - Map
  #   - Struct (defined in the Bolt protocol)
  @fallback_to_any true

  @doc """
  Encode entity into its Bolt binary represenation depending of the used bolt version
  """

  @spec encode(any(), integer()) :: binary()
  def encode(entity, bolt_version)
end

defimpl PackStream.Encoder, for: Atom do
  def encode(data, bolt_version), do: MainEncoder.encode_atom( data, bolt_version)
end

defimpl PackStream.Encoder, for: BitString do
  def encode(data, bolt_version), do: MainEncoder.encode_string( data, bolt_version)
end

defimpl PackStream.Encoder, for: Integer do
  def encode(data, bolt_version), do:  MainEncoder.encode_integer( data, bolt_version)
end

defimpl PackStream.Encoder, for: Float do
  def encode(data, bolt_version), do:  MainEncoder.encode_float( data, bolt_version)
end

defimpl PackStream.Encoder, for: List do
  def encode(data, bolt_version), do:  MainEncoder.encode_list(data, bolt_version)
end

defimpl PackStream.Encoder, for: Map do
  def encode(data, bolt_version), do:  MainEncoder.encode_map( data, bolt_version)
end

defimpl PackStream.Encoder, for: Time do
  def encode(data, bolt_version), do:  MainEncoder.encode_local_time( data, bolt_version)
end

defimpl PackStream.Encoder, for: Bolt.Sips.Types.TimeWithTZOffset do
  def encode(data, bolt_version) do
    MainEncoder.encode_time_with_tz( data, bolt_version)
  end
end

defimpl PackStream.Encoder, for: Date do
  def encode(data, bolt_version), do: MainEncoder.encode_date( data, bolt_version)
end

defimpl PackStream.Encoder, for: NaiveDateTime do
  def encode(data, bolt_version) do
    MainEncoder.encode_local_datetime( data, bolt_version)
  end
end

defimpl PackStream.Encoder, for: DateTime do
  def encode(data, version) do
    MainEncoder.encode_datetime_with_tz_id( data, version)
  end
end

defimpl PackStream.Encoder, for: Bolt.Sips.Types.DateTimeWithTZOffset do
  def encode(data, version) do
    MainEncoder.encode_datetime_with_tz_offset( data, version)
  end
end

defimpl PackStream.Encoder, for: Bolt.Sips.Types.Duration do
  def encode(data, version), do: MainEncoder.encode_duration( data, version)
end

defimpl PackStream.Encoder, for: Bolt.Sips.Types.Point do
  def encode(data, version), do: MainEncoder.encode_point( data, version)
end

defimpl PackStream.Encoder, for: Any do
  @spec encode({integer(), list()} | %{:__struct__ => String.t()}, integer()) ::
          Bolt.Sips.Internals.PackStream.value() | <<_::16, _::_*8>>
  def encode({signature, data}, bolt_version) when is_list(data) do
    valid_signatures =
      PackStream.Message.Encoder.valid_signatures(bolt_version) ++
        Bolt.Sips.Internals.PackStream.MarkersHelper.valid_signatures()

    if signature in valid_signatures do
      MainEncoder.encode_struct( {signature, data}, bolt_version)
    else
      raise Bolt.Sips.Internals.PackStreamError,
        message: "Unable to encode",
        data: data,
        bolt_version: bolt_version
    end
  end

  # Elixir structs just need to be convertedd to map befoare being encoded
  def encode(%{__struct__: _} = data, bolt_version) do
    map = Map.from_struct(data)
    PackStream.Encoder.encode(map, bolt_version)
  end

  def encode(data, bolt_version) do
    raise Bolt.Sips.Internals.PackStreamError,
      message: "Unable to encode",
      data: data,
      bolt_version: bolt_version
  end
end
