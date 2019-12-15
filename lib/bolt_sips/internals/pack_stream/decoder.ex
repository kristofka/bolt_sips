defmodule Bolt.Sips.Internals.PackStream.Decoder do
  @moduledoc false
  _moduledoc = """
  This module is responsible for dispatching decoding amongst decoder depending on the
  used bolt version.

  Most of the documentation regarding Bolt binary format can be found in
  `Bolt.Sips.Internals.PackStream.EncoderV1` and `Bolt.Sips.Internals.PackStream.EncoderV2`.

  Here will be found ocumenation about data that are only availalbe for decoding::
  - Node
  - Relationship
  - Unbound relationship
  - Path
  """

  alias Bolt.Sips.Internals.BoltVersionHelper
  alias Bolt.Sips.Internals.PackStreamError
  alias Bolt.Sips.Types
  alias Bolt.Sips.Types.{TimeWithTZOffset, DateTimeWithTZOffset, Duration, Point}
  @available_bolt_versions BoltVersionHelper.available_versions()

  # Null
  @null_marker 0xC0

  # Boolean
  @true_marker 0xC3
  @false_marker 0xC2

  # String
  @tiny_bitstring_marker 0x8
  @bitstring8_marker 0xD0
  @bitstring16_marker 0xD1
  @bitstring32_marker 0xD2

  # Integer
  @int8_marker 0xC8
  @int16_marker 0xC9
  @int32_marker 0xCA
  @int64_marker 0xCB

  # Float
  @float_marker 0xC1

  # List
  @tiny_list_marker 0x9
  @list8_marker 0xD4
  @list16_marker 0xD5
  @list32_marker 0xD6

  # Map
  @tiny_map_marker 0xA
  @map8_marker 0xD8
  @map16_marker 0xD9
  @map32_marker 0xDA

  # Structure
  @tiny_struct_marker 0xB
  @struct8_marker 0xDC
  @struct16_marker 0xDD

  # Node
  @node_marker 0x4E

  # Relationship
  @relationship_marker 0x52

  # Unbounded relationship
  @unbounded_relationship_marker 0x72

  # Path
  @path_marker 0x50

  # Local Time
  @local_time_signature 0x74
  @local_time_struct_size 1

  # Time With TZ Offset
  @time_with_tz_signature 0x54
  @time_with_tz_struct_size 2

  # Date
  @date_signature 0x44
  @date_struct_size 1

  # Local DateTime
  @local_datetime_signature 0x64
  @local_datetime_struct_size 2

  # Datetime with TZ offset
  @datetime_with_zone_offset_signature 0x46
  @datetime_with_zone_offset_struct_size 3

  # Datetime with TZ id
  @datetime_with_zone_id_signature 0x66
  @datetime_with_zone_id_struct_size 3

  # Duration
  @duration_signature 0x45
  @duration_struct_size 4

  # Point 2D
  @point2d_signature 0x58
  @point2d_struct_size 3

  # Point 3D
  @point3d_signature 0x59
  @point3d_struct_size 4

  @spec decode(binary() | {integer(), binary(), integer()}, integer()) ::
          list() | {:error, :not_implemented}
  def decode(<<@null_marker, rest::binary>>, bolt_version) do
    [nil | decode(rest, bolt_version)]
  end

  # Boolean
  def decode(<<@true_marker, rest::binary>>, bolt_version) do
    [true | decode(rest, bolt_version)]
  end

  def decode(<<@false_marker, rest::binary>>, bolt_version) do
    [false | decode(rest, bolt_version)]
  end

  # Float
  def decode(<<@float_marker, number::float, rest::binary>>, bolt_version) do
    [number | decode(rest, bolt_version)]
  end

  # Strings
  def decode(<<@tiny_bitstring_marker::4, str_length::4, rest::bytes>>, bolt_version) do
    decode_string(rest, str_length, bolt_version)
  end

  def decode(<<@bitstring8_marker, str_length, rest::bytes>>, bolt_version) do
    decode_string(rest, str_length, bolt_version)
  end

  def decode(<<@bitstring16_marker, str_length::16, rest::bytes>>, bolt_version) do
    decode_string(rest, str_length, bolt_version)
  end

  def decode(<<@bitstring32_marker, str_length::32, rest::binary>>, bolt_version) do
    decode_string(rest, str_length, bolt_version)
  end

  # Lists
  def decode(<<@tiny_list_marker::4, list_size::4>> <> bin, bolt_version) do
    decode_list(bin, list_size, bolt_version)
  end

  def decode(<<@list8_marker, list_size::8>> <> bin, bolt_version) do
    decode_list(bin, list_size, bolt_version)
  end

  def decode(<<@list16_marker, list_size::16>> <> bin, bolt_version) do
    decode_list(bin, list_size, bolt_version)
  end

  def decode(<<@list32_marker, list_size::32>> <> bin, bolt_version) do
    decode_list(bin, list_size, bolt_version)
  end

  # Maps
  def decode(<<@tiny_map_marker::4, entries::4>> <> bin, bolt_version) do
    decode_map(bin, entries, bolt_version)
  end

  def decode(<<@map8_marker, entries::8>> <> bin, bolt_version) do
    decode_map(bin, entries, bolt_version)
  end

  def decode(<<@map16_marker, entries::16>> <> bin, bolt_version) do
    decode_map(bin, entries, bolt_version)
  end

  def decode(<<@map32_marker, entries::32>> <> bin, bolt_version) do
    decode_map(bin, entries, bolt_version)
  end

  # Struct
  def decode(<<@tiny_struct_marker::4, struct_size::4, sig::8>> <> struct, bolt_version) do
    decode({sig, struct, struct_size}, bolt_version)
  end

  def decode(<<@struct8_marker, struct_size::8, sig::8>> <> struct, bolt_version) do
    decode({sig, struct, struct_size}, bolt_version)
  end

  def decode(<<@struct16_marker, struct_size::16, sig::8>> <> struct, bolt_version) do
    decode({sig, struct, struct_size}, bolt_version)
  end

  ######### SPECIAL STRUCTS

  # Node
  def decode({@node_marker, struct, struct_size}, bolt_version) do
    {[id, labels, props], rest} = decode_struct(struct, struct_size, bolt_version)

    node = %Types.Node{id: id, labels: labels, properties: props}

    [node | rest]
  end

  # Relationship
  def decode({@relationship_marker, struct, struct_size}, bolt_version) do
    {[id, start_node, end_node, type, props], rest} =
      decode_struct(struct, struct_size, bolt_version)

    relationship = %Types.Relationship{
      id: id,
      start: start_node,
      end: end_node,
      type: type,
      properties: props
    }

    [relationship | rest]
  end

  # UnboundedRelationship
  def decode({@unbounded_relationship_marker, struct, struct_size}, bolt_version) do
    {[id, type, props], rest} = decode_struct(struct, struct_size, bolt_version)

    unbounded_relationship = %Types.UnboundRelationship{
      id: id,
      type: type,
      properties: props
    }

    [unbounded_relationship | rest]
  end

  # Path
  def decode({@path_marker, struct, struct_size}, bolt_version) do
    {[nodes, relationships, sequence], rest} =
      decode_struct(struct, struct_size, bolt_version)

    path = %Types.Path{
      nodes: nodes,
      relationships: relationships,
      sequence: sequence
    }

    [path | rest]
  end

  # Manage the end of data
  def decode("", _), do: []

  # Integers
  def decode(<<@int8_marker, int::signed-integer, rest::binary>>, bolt_version) do
    [int | decode(rest, bolt_version)]
  end

  def decode(<<@int16_marker, int::signed-integer-16, rest::binary>>, bolt_version) do
    [int | decode(rest, bolt_version)]
  end

  def decode(<<@int32_marker, int::signed-integer-32, rest::binary>>, bolt_version) do
    [int | decode(rest, bolt_version)]
  end

  def decode(<<@int64_marker, int::signed-integer-64, rest::binary>>, bolt_version) do
    [int | decode(rest, bolt_version)]
  end

  def decode(<<int::signed-integer, rest::binary>>, bolt_version) do
    [int | decode(rest, bolt_version)]
  end

  # Local Date
  @spec decode({integer(), binary(), integer()}, integer()) :: list() | {:error, :not_implemented}
  def decode({@date_signature, struct, @date_struct_size}, bolt_version) do
    {[date], rest} = decode_struct(struct, @date_struct_size, bolt_version)
    [Date.add(~D[1970-01-01], date) | rest]
  end

  # Local Time
  def decode({@local_time_signature, struct, @local_time_struct_size}, bolt_version) do
    {[time], rest} = decode_struct(struct, @local_time_struct_size, bolt_version)

    [Time.add(~T[00:00:00.000], time, :nanosecond) | rest]
  end

  # Local DateTime
  def decode({@local_datetime_signature, struct, @local_datetime_struct_size}, bolt_version) do
    {[seconds, nanoseconds], rest} =
      decode_struct(struct, @local_datetime_struct_size, bolt_version)

    ndt =
      NaiveDateTime.add(
        ~N[1970-01-01 00:00:00.000],
        seconds * 1_000_000_000 + nanoseconds,
        :nanosecond
      )

    [ndt | rest]
  end

  # Time with Zone Offset
  def decode({@time_with_tz_signature, struct, @time_with_tz_struct_size}, bolt_version) do
    {[time, offset], rest} =
      decode_struct(struct, @time_with_tz_struct_size, bolt_version)

    t = TimeWithTZOffset.create(Time.add(~T[00:00:00.000], time, :nanosecond), offset)
    [t | rest]
  end

  # Datetime with zone Id
  def decode(
        {@datetime_with_zone_id_signature, struct, @datetime_with_zone_id_struct_size},
        bolt_version
      ) do
    {[seconds, nanoseconds, zone_id], rest} =
      decode_struct(struct, @datetime_with_zone_id_struct_size, bolt_version)

    naive_dt =
      NaiveDateTime.add(
        ~N[1970-01-01 00:00:00.000],
        seconds * 1_000_000_000 + nanoseconds,
        :nanosecond
      )

    dt = Bolt.Sips.TypesHelper.datetime_with_micro(naive_dt, zone_id)
    [dt | rest]
  end

  # Datetime with zone offset
  def decode(
        {@datetime_with_zone_offset_signature, struct, @datetime_with_zone_offset_struct_size},
        bolt_version
      ) do
    {[seconds, nanoseconds, zone_offset], rest} =
      decode_struct(struct, @datetime_with_zone_id_struct_size, bolt_version)

    naive_dt =
      NaiveDateTime.add(
        ~N[1970-01-01 00:00:00.000],
        seconds * 1_000_000_000 + nanoseconds,
        :nanosecond
      )

    dt = DateTimeWithTZOffset.create(naive_dt, zone_offset)
    [dt | rest]
  end

  # Duration
  def decode({@duration_signature, struct, @duration_struct_size}, bolt_version) do
    {[months, days, seconds, nanoseconds], rest} =
      decode_struct(struct, @duration_struct_size, bolt_version)

    duration = Duration.create(months, days, seconds, nanoseconds)
    [duration | rest]
  end

  # Point2D
  def decode({@point2d_signature, struct, @point2d_struct_size}, bolt_version) do
    {[srid, x, y], rest} = decode_struct(struct, @point2d_struct_size, bolt_version)
    point = Point.create(srid, x, y)

    [point | rest]
  end

  # Point3D
  def decode({@point3d_signature, struct, @point3d_struct_size}, bolt_version) do
    {[srid, x, y, z], rest} = decode_struct(struct, @point3d_struct_size, bolt_version)
    point = Point.create(srid, x, y, z)

    [point | rest]
  end

  def decode(_, _) do
    {:error, :not_implemented}
  end



  @doc """
  Decodes a struct
  """
  @spec decode_struct(binary(), integer(), integer()) :: {list(), list()}
  def decode_struct(struct, struct_size, bolt_version) do
    struct
    |> decode(bolt_version)
    |> Enum.split(struct_size)
  end

  @spec to_map(list()) :: map()
  defp to_map(map) do
    map
    |> Enum.chunk_every(2)
    |> Enum.map(&List.to_tuple/1)
    |> Map.new()
  end

  @spec decode_string(binary(), integer(), integer()) :: list()
  defp decode_string(bytes, str_length, bolt_version) do
    <<string::binary-size(str_length), rest::binary>> = bytes

    [string | decode(rest, bolt_version)]
  end

  @spec decode_list(binary(), integer(), integer()) :: list()
  defp decode_list(list, list_size, bolt_version) do
    {list, rest} = list |> decode(bolt_version) |> Enum.split(list_size)
    [list | rest]
  end

  @spec decode_map(binary(), integer(), integer()) :: list()
  defp decode_map(map, entries, bolt_version) do
    {map, rest} = map |> decode(bolt_version) |> Enum.split(entries * 2)

    [to_map(map) | rest]
  end
end
