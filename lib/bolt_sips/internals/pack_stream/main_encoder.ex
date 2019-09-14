defmodule Bolt.Sips.Internals.PackStream.MainEncoder do
  @moduledoc false
  alias Bolt.Sips.Internals.PackStream.Encoder
  alias Bolt.Sips.Types.{TimeWithTZOffset, DateTimeWithTZOffset, Duration, Point}

  @int8 -127..-17
  @int16_low -32_768..-129
  @int16_high 128..32_767
  @int32_low -2_147_483_648..-32_769
  @int32_high 32_768..2_147_483_647
  @int64_low -9_223_372_036_854_775_808..-2_147_483_649
  @int64_high 2_147_483_648..9_223_372_036_854_775_807
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

  @doc """
  Encode an atom into Bolt binary format.

  Encoding:
  `Marker`

  with

  | Value | Marker |
  | ------- | -------- |
  | nil | `0xC0` |
  | false | `0xC2` |
  | true | `0xC3` |

  Other atoms are converted to string before encoding.

  ## Example

      iex> alias Bolt.Sips.Internals.PackStream.EncoderV1
      iex> EncoderV1.encode_atom(nil, 1)
      <<0xC0>>
      iex> EncoderV1.encode_atom(true, 1)
      <<0xC3>>
      iex> EncoderV1.encode_atom(:guten_tag, 1)
      <<0x89, 0x67, 0x75, 0x74, 0x65, 0x6E, 0x5F, 0x74, 0x61, 0x67>>
  """
  @spec encode_atom(atom(), integer()) :: Bolt.Sips.Internals.PackStream.value()
  def encode_atom(nil, _bolt_version), do: <<@null_marker>>
  def encode_atom(true, _bolt_version), do: <<@true_marker>>
  def encode_atom(false, _bolt_version), do: <<@false_marker>>

  def encode_atom(other, bolt_version) do
    other |> Atom.to_string() |> encode_string(bolt_version)
  end

  @doc """
  Encode a string into Bolt binary format.

  Encoding:
  `Marker` `Size` `Content`

  with

  | Marker | Size | Max data size |
  |--------|------|---------------|
  | `0x80`..`0x8F` | None (contained in marker) | 15 bytes |
  | `0xD0` | 8-bit integer | 255 bytes |
  | `0xD1` | 16-bit integer | 65_535 bytes |
  | `0xD2` | 32-bit integer | 4_294_967_295 bytes |

  ## Example

      iex> alias Bolt.Sips.Internals.PackStream.EncoderV1
      iex> EncoderV1.encode_string("guten tag", 1)
      <<0x89, 0x67, 0x75, 0x74, 0x65, 0x6E, 0x20, 0x74, 0x61, 0x67>>
  """
  @spec encode_string(String.t(), integer()) :: Bolt.Sips.Internals.PackStream.value()
  def encode_string(string, _bolt_version) when byte_size(string) <= 15 do
    [<<@tiny_bitstring_marker::4, byte_size(string)::4>>, string]
  end

  def encode_string(string, _bolt_version) when byte_size(string) <= 255 do
    [<<@bitstring8_marker, byte_size(string)::8>>, string]
  end

  def encode_string(string, _bolt_version) when byte_size(string) <= 65_535 do
    [<<@bitstring16_marker, byte_size(string)::16>> , string]
  end

  def encode_string(string, _bolt_version) when byte_size(string) <= 4_294_967_295 do
    [<<@bitstring32_marker, byte_size(string)::32>>, string]
  end

  @doc """
  Encode an integer into Bolt binary format.

  Encoding:
  `Marker` `Value`

  with

  |   | Marker |
  |---|--------|
  | tiny int | `0x2A` |
  | int8 | `0xC8` |
  | int16 | `0xC9` |
  | int32 | `0xCA` |
  | int64 | `0xCB` |

  ## Example

      iex> alias Bolt.Sips.Internals.PackStream.EncoderV1
      iex> EncoderV1.encode_integer(74, 1)
      <<0x4A>>
      iex> EncoderV1.encode_integer(-74_789, 1)
      <<0xCA, 0xFF, 0xFE, 0xDB, 0xDB>>
  """
  @spec encode_integer(integer(), integer()) :: Bolt.Sips.Internals.PackStream.value()
  def encode_integer(integer, _bolt_version) when integer in -16..127 do
    <<integer>>
  end

  def encode_integer(integer, _bolt_version) when integer in @int8 do
    <<@int8_marker, integer>>
  end

  def encode_integer(integer, _bolt_version)
      when integer in @int16_low or integer in @int16_high do
    <<@int16_marker, integer::16>>
  end

  def encode_integer(integer, _bolt_version)
      when integer in @int32_low or integer in @int32_high do
    <<@int32_marker, integer::32>>
  end

  def encode_integer(integer, _bolt_version)
      when integer in @int64_low or integer in @int64_high do
    <<@int64_marker, integer::64>>
  end

  @doc """
  Encode a float into Bolt binary format.

  Encoding: `Marker` `8 byte Content`.

  Marker: `0xC1`

  Formated according to the IEEE 754 floating-point "double format" bit layout.

  ## Example

      iex> alias Bolt.Sips.Internals.PackStream.EncoderV1
      iex> EncoderV1.encode_float(42.42, 1)
      <<0xC1, 0x40, 0x45, 0x35, 0xC2, 0x8F, 0x5C, 0x28, 0xF6>>
  """
  @spec encode_float(float(), integer()) :: Bolt.Sips.Internals.PackStream.value()
  def encode_float(number, _bolt_version), do: <<@float_marker, number::float>>

  @doc """
  Encode a list into Bolt binary format.

  Encoding:
  `Marker` `Size` `Content`

  with

  | Marker | Size | Max list size |
  |--------|------|---------------|
  | `0x90`..`0x9F` | None (contained in marker) | 15 bytes |
  | `0xD4` | 8-bit integer | 255 items |
  | `0xD5` | 16-bit integer | 65_535 items |
  | `0xD6` | 32-bit integer | 4_294_967_295 items |

  ## Example

      iex> alias Bolt.Sips.Internals.PackStream.EncoderV1
      iex> EncoderV1.encode_list(["hello", "world"], 1)
      <<0x92, 0x85, 0x68, 0x65, 0x6C, 0x6C, 0x6F, 0x85, 0x77, 0x6F, 0x72, 0x6C, 0x64>>
  """
  @spec encode_list(list(), integer()) :: Bolt.Sips.Internals.PackStream.value()
  def encode_list(list, bolt_version) when length(list) <= 15 do
    [<<@tiny_list_marker::4, length(list)::4>>,  encode_list_data(list, bolt_version)]
  end

  def encode_list(list, bolt_version) when length(list) <= 255 do
    [<<@list8_marker, length(list)::8>>,  encode_list_data(list, bolt_version)]
  end

  def encode_list(list, bolt_version) when length(list) <= 65_535 do
    [<<@list16_marker, length(list)::16>> , encode_list_data(list, bolt_version)]
  end

  def encode_list(list, bolt_version) when length(list) <= 4_294_967_295 do
    [<<@list32_marker, length(list)::32>> , encode_list_data(list, bolt_version)]
  end

  @spec encode_list_data(list(), integer()) :: binary()
  defp encode_list_data(data, bolt_version) do
    Enum.map(data, &Encoder.encode(&1, bolt_version))
  end

  @doc """
  Encode a map into Bolt binary format.

  Note that Elixir structs are converted to map for encoding purpose.

  Encoding:
  `Marker` `Size` `Content`

  with

  | Marker | Size | Max map size |
  |--------|------|---------------|
  | `0xA0`..`0xAF` | None (contained in marker) | 15 entries |
  | `0xD8` | 8-bit integer | 255 entries |
  | `0xD9` | 16-bit integer | 65_535 entries |
  | `0xDA` | 32-bit integer | 4_294_967_295 entries |

  ## Example

      iex> alias Bolt.Sips.Internals.PackStream.EncoderV1
      iex> EncoderV1.encode_map(%{id: 345, value: "hello world"}, 1)
      <<0xA2, 0x82, 0x69, 0x64, 0xC9, 0x1, 0x59, 0x85, 0x76, 0x61, 0x6C, 0x75,
      0x65, 0x8B, 0x68, 0x65, 0x6C, 0x6C, 0x6F, 0x20, 0x77, 0x6F, 0x72, 0x6C, 0x64>>
  """
  @spec encode_map(map(), integer()) :: Bolt.Sips.Internals.PackStream.value()
  def encode_map(map, bolt_version) when map_size(map) <= 15 do
    [<<@tiny_map_marker::4, map_size(map)::4>> , encode_kv(map, bolt_version)]
  end

  def encode_map(map, bolt_version) when map_size(map) <= 255 do
    [<<@map8_marker, map_size(map)::8>> , encode_kv(map, bolt_version)]
  end

  def encode_map(map, bolt_version) when map_size(map) <= 65_535 do
    [<<@map16_marker, map_size(map)::16>> , encode_kv(map, bolt_version)]
  end

  def encode_map(map, bolt_version) when map_size(map) <= 4_294_967_295 do
    [<<@map32_marker, map_size(map)::32>> , encode_kv(map, bolt_version)]
  end

  @spec encode_kv(map(), integer()) :: binary()
  defp encode_kv(map, bolt_version) do
    Enum.reduce(map, <<>>, fn data, acc ->
      [acc,  do_reduce_kv(data, bolt_version)]
    end)
  end

  @spec do_reduce_kv({atom(), any()}, integer()) :: binary()
  defp do_reduce_kv({key, value}, bolt_version) do
    [Encoder.encode(key, bolt_version) , Encoder.encode(value, bolt_version)]
  end

  @doc """
  Encode a struct into Bolt binary format.
  This concerns Bolt Structs as defined in []().
  Elixir structs are just converted to regular maps before encoding

  Encoding:
  `Marker` `Size` `Signature` `Content`

  with

  | Marker | Size | Max structure size |
  |--------|------|---------------|
  | `0xB0`..`0xBF` | None (contained in marker) | 15 fields |
  | `0xDC` | 8-bit integer | 255 fields |
  | `0xDD` | 16-bit integer | 65_535 fields |

  ## Example

      iex> alias Bolt.Sips.Internals.PackStream.EncoderV1
      iex> EncoderV1.encode_struct({0x01, ["two", "params"]}, 1)
      <<0xB2, 0x1, 0x83, 0x74, 0x77, 0x6F, 0x86, 0x70, 0x61, 0x72, 0x61, 0x6D, 0x73>>

  """
  @spec encode_struct({integer(), list()}, integer()) :: Bolt.Sips.Internals.PackStream.value()
  def encode_struct({signature, list}, bolt_version) when length(list) <= 15 do
   [ <<@tiny_struct_marker::4, length(list)::4, signature>> , encode_list_data(list, bolt_version)]
  end

  def encode_struct({signature, list}, bolt_version) when length(list) <= 255 do
    [<<@struct8_marker::8, length(list)::8, signature>> , encode_list_data(list, bolt_version)]
  end

  def encode_struct({signature, list}, bolt_version) when length(list) <= 65_535 do
    [<<@struct16_marker::8, length(list)::16, signature>> , encode_list_data(list, bolt_version)]
  end

  @doc """
  Encode a Time (represented by Time) into Bolt binary format.
  Encoded in a structure.

  Signature: `0x74`

  Encoding:
  `Marker` `Size` `Signature` ` Content`

  where `Content` is:
  `Nanoseconds_from_00:00:00`

  ## Example

      iex> Bolt.Sips.Internals.PackStream.EncoderV2.encode_local_time(~T[06:54:32.453], 2)
      <<0xB1, 0x74, 0xCB, 0x0, 0x0, 0x16, 0x9F, 0x11, 0xB9, 0xCB, 0x40>>
  """
  @spec encode_local_time(Time.t(), integer()) :: Bolt.Sips.Internals.PackStream.value()
  def encode_local_time(local_time, bolt_version) do
    Encoder.encode({@local_time_signature, [day_time(local_time)]}, bolt_version)
  end

  @doc """
  Encode a TIME WITH TIMEZONE OFFSET (represented by TimeWithTZOffset) into Bolt binary format.
  Encoded in a structure.

  Signature: `0x54`

  Encoding:
  `Marker` `Size` `Signature` ` Content`

  where `Content` is:
  `Nanoseconds_from_00:00:00` `Offset_in_seconds`

  ## Example

      iex> time_with_tz = Bolt.Sips.Types.TimeWithTZOffset.create(~T[06:54:32.453], 3600)
      iex> Bolt.Sips.Internals.PackStream.EncoderV2.encode_time_with_tz(time_with_tz, 2)
      <<0xB2, 0x54, 0xCB, 0x0, 0x0, 0x16, 0x9F, 0x11, 0xB9, 0xCB, 0x40, 0xC9, 0xE, 0x10>>
  """
  def encode_time_with_tz(%TimeWithTZOffset{time: time, timezone_offset: offset}, bolt_version) do
    Encoder.encode({@time_with_tz_signature, [day_time(time), offset]}, bolt_version)
  end

  @spec day_time(Time.t()) :: integer()
  defp day_time(time) do
    Time.diff(time, ~T[00:00:00.000], :nanosecond)
  end

  @doc """
  Encode a DATE (represented by Date) into Bolt binary format.
  Encoded in a structure.

  Signature: `0x44`

  Encoding:
  `Marker` `Size` `Signature` ` Content`

  where `Content` is:
  `Nb_days_since_epoch`

  ## Example

      iex> Bolt.Sips.Internals.PackStream.EncoderV2.encode_date(~D[2019-04-23], 2)
      <<0xB1, 0x44, 0xC9, 0x46, 0x59>>

  """
  @spec encode_date(Date.t(), integer()) :: Bolt.Sips.Internals.PackStream.value()
  def encode_date(date, bolt_version) do
    epoch = Date.diff(date, ~D[1970-01-01])

    Encoder.encode({@date_signature, [epoch]}, bolt_version)
  end

  @doc """
  Encode a LOCAL DATETIME (Represented by NaiveDateTime) into Bolt binary format.
  Encoded in a structure.

  WARNING: Nanoseconds are left off as NaiveDateTime doesn't handle them.
  A new Calendar should be implemented to manage them.

  Signature: `0x64`

  Encoding:
  `Marker` `Size` `Signature` ` Content`

  where `Content` is:
  `Nb_seconds_since_epoch` `Remainder_in_nanoseconds`

  ## Example

      iex> Bolt.Sips.Internals.PackStream.EncoderV2.encode_local_datetime(~N[2019-04-23 13:45:52.678], 2)
      <<0xB2, 0x64, 0xCA, 0x5C, 0xBF, 0x17, 0x10, 0xCA, 0x28, 0x69, 0x75, 0x80>>

  """
  @spec encode_local_datetime(Calendar.naive_datetime(), integer()) ::
          Bolt.Sips.Internals.PackStream.value()
  def encode_local_datetime(local_datetime, bolt_version) do
    Encoder.encode({@local_datetime_signature, decompose_datetime(local_datetime)}, bolt_version)
  end

  @doc """
  Encode DATETIME WITH TIMEZONE ID (represented by Calendar.DateTime) into Bolt binary format.
  Encoded in a structure.

  WARNING: Nanoseconds are left off as NaiveDateTime doesn't handle them.
  A new Calendar should be implemented to manage them.

  Signature: `0x66`

  Encoding:
  `Marker` `Size` `Signature` ` Content`

  where `Content` is:
  `Nb_seconds_since_epoch` `Remainder_in_nanoseconds` `Zone_id`

  ## Example

      iex> d = Bolt.Sips.TypesHelper.datetime_with_micro(~N[2013-11-12 07:32:02.003],
      ...> "Europe/Berlin")
      #DateTime<2013-11-12 07:32:02.003+01:00 CET Europe/Berlin>
      iex> Bolt.Sips.Internals.PackStream.EncoderV2.encode_datetime_with_tz_id(d, 2)
      <<0xB3, 0x66, 0xCA, 0x52, 0x81, 0xD9, 0x72, 0xCA, 0x0, 0x2D, 0xC6, 0xC0, 0x8D, 0x45, 0x75,
      0x72, 0x6F, 0x70, 0x65, 0x2F, 0x42, 0x65, 0x72, 0x6C, 0x69, 0x6E>>

  """
  @spec encode_datetime_with_tz_id(Calendar.datetime(), integer()) ::
          Bolt.Sips.Internals.PackStream.value()
  def encode_datetime_with_tz_id(datetime, bolt_version) do
    data = decompose_datetime(DateTime.to_naive(datetime)) ++ [datetime.time_zone]

    Encoder.encode({@datetime_with_zone_id_signature, data}, bolt_version)
  end

  @doc """
  Encode DATETIME WITH TIMEZONE OFFSET (represented by DateTimeWithTZOffset) into Bolt binary format.
  Encoded in a structure.

  WARNING: Nanoseconds are left off as NaiveDateTime doesn't handle them.
  A new Calendar should be implemented to manage them.

  Signature: `0x46`

  Encoding:
  `Marker` `Size` `Signature` ` Content`

  where `Content` is:
  `Nb_seconds_since_epoch` `Remainder_in_nanoseconds` `Zone_offset`

  ## Example

      iex> d = Bolt.Sips.Types.DateTimeWithTZOffset.create(~N[2013-11-12 07:32:02.003], 7200)
      %Bolt.Sips.Types.DateTimeWithTZOffset{
              naive_datetime: ~N[2013-11-12 07:32:02.003],
              timezone_offset: 7200
            }
      iex> Bolt.Sips.Internals.PackStream.EncoderV2.encode_datetime_with_tz_offset(d, 2)
      <<0xB3, 0x46, 0xCA, 0x52, 0x81, 0xD9, 0x72, 0xCA, 0x0, 0x2D, 0xC6, 0xC0, 0xC9, 0x1C, 0x20>>

  """
  @spec encode_datetime_with_tz_offset(DateTimeWithTZOffset.t(), integer()) ::
          Bolt.Sips.Internals.PackStream.value()
  def encode_datetime_with_tz_offset(
        %DateTimeWithTZOffset{naive_datetime: ndt, timezone_offset: tz_offset},
        bolt_version
      ) do
    data = decompose_datetime(ndt) ++ [tz_offset]
    Encoder.encode({@datetime_with_zone_offset_signature, data}, bolt_version)
  end

  @spec decompose_datetime(Calendar.naive_datetime()) :: [integer()]
  defp decompose_datetime(%NaiveDateTime{} = datetime) do
    datetime_micros = NaiveDateTime.diff(datetime, ~N[1970-01-01 00:00:00.000], :microsecond)

    seconds = div(datetime_micros, 1_000_000)
    nanoseconds = rem(datetime_micros, 1_000_000) * 1_000

    [seconds, nanoseconds]
  end

  @doc """
  Encode DURATION (represented by Duration) into Bolt binary format.
  Encoded in a structure.

  Signature: `0x45`

  Encoding:
  `Marker` `Size` `Signature` ` Content`

  where `Content` is:
  `Months` `Days` `Seconds` `Nanoseconds`

  ## Example

      iex(60)> d = %Bolt.Sips.Types.Duration{
      ...(60)>   years: 3,
      ...(60)>   months: 1,
      ...(60)>   weeks: 7,
      ...(60)>   days: 4,
      ...(60)>   hours: 13,
      ...(60)>   minutes: 2,
      ...(60)>   seconds: 21,
      ...(60)>   nanoseconds: 554
      ...(60)> }
      %Bolt.Sips.Types.Duration{
        days: 4,
        hours: 13,
        minutes: 2,
        months: 1,
        nanoseconds: 554,
        seconds: 21,
        weeks: 7,
        years: 3
      }
      iex> Bolt.Sips.Internals.PackStream.EncoderV2.encode_duration(d, 2)
      <<0xB4, 0x45, 0x25, 0x35, 0xCA, 0x0, 0x0, 0xB7, 0x5D, 0xC9, 0x2, 0x2A>>
  """
  @spec encode_duration(Duration.t(), integer()) :: Bolt.Sips.Internals.PackStream.value()
  def encode_duration(%Duration{} = duration, bolt_version) do
    Encoder.encode({@duration_signature, compact_duration(duration)}, bolt_version)
  end

  @spec compact_duration(Duration.t()) :: [integer()]
  defp compact_duration(%Duration{} = duration) do
    months = 12 * duration.years + duration.months
    days = 7 * duration.weeks + duration.days
    seconds = 3600 * duration.hours + 60 * duration.minutes + duration.seconds

    [months, days, seconds, duration.nanoseconds]
  end

  @doc """
  Encode POINT 2D & 3D (represented by Point) into Bolt binary format.
  Encoded in a structure.


  ## Point 2D
  Signature: `0x58`

  Encoding:
  `Marker` `Size` `Signature` ` Content`

  where `Content` is:
  `SRID` `x_or_longitude` `y_or_latitude`

  ## Example

      iex> p = Bolt.Sips.Types.Point.create(:wgs_84, 65.43, 12.54)
      %Bolt.Sips.Types.Point{
              crs: "wgs-84",
              height: nil,
              latitude: 12.54,
              longitude: 65.43,
              srid: 4326,
              x: 65.43,
              y: 12.54,
              z: nil
            }
      iex> Bolt.Sips.Internals.PackStream.EncoderV2.encode_point(p, 2)
      <<0xB3, 0x58, 0xC9, 0x10, 0xE6, 0xC1, 0x40, 0x50, 0x5B, 0x85, 0x1E, 0xB8, 0x51, 0xEC, 0xC1,
      0x40, 0x29, 0x14, 0x7A, 0xE1, 0x47, 0xAE, 0x14>>

  ## Point 3D
  Signature: `0x58`

  Encoding:
  `Marker` `Size` `Signature` ` Content`

  where `Content` is:
  `SRID` `x_or_longitude` `y_or_latitude` `z_or_height`

  ## Example

      iex> p = Bolt.Sips.Types.Point.create(:wgs_84, 45.0003, 40.3245, 23.1)
      %Bolt.Sips.Types.Point{
              crs: "wgs-84-3d",
              height: 23.1,
              latitude: 40.3245,
              longitude: 45.0003,
              srid: 4979,
              x: 45.0003,
              y: 40.3245,
              z: 23.1
            }
      iex> Bolt.Sips.Internals.PackStream.EncoderV2.encode_point(p, 2)
      <<0xB4, 0x59, 0xC9, 0x13, 0x73, 0xC1, 0x40, 0x46, 0x80, 0x9, 0xD4, 0x95, 0x18, 0x2B, 0xC1,
      0x40, 0x44, 0x29, 0x89, 0x37, 0x4B, 0xC6, 0xA8, 0xC1, 0x40, 0x37, 0x19, 0x99, 0x99, 0x99,
      0x99, 0x9A>>

  """
  @spec encode_point(Point.t(), integer()) :: Bolt.Sips.Internals.PackStream.value()
  def encode_point(%Point{z: nil} = point, bolt_version) do
    Encoder.encode({@point2d_signature, [point.srid, point.x, point.y]}, bolt_version)
  end

  def encode_point(%Point{} = point, bolt_version) do
    Encoder.encode({@point3d_signature, [point.srid, point.x, point.y, point.z]}, bolt_version)
  end
end
