defmodule Bolt.Sips.Internals.PackStream.EncoderHelper do
  @moduledoc false
  alias Bolt.Sips.Internals.PackStreamError
  alias Bolt.Sips.Internals.BoltVersionHelper
  alias Bolt.Sips.Internals.PackStream.Encoder

  use Bolt.Sips.Internals.PackStream.V1Utils
  use Bolt.Sips.Internals.PackStream.V2Utils
  use Bolt.Sips.Internals.PackStream.V1
  use Bolt.Sips.Internals.PackStream.V2

  @available_bolt_versions BoltVersionHelper.available_versions()


  @doc """
  For the given `data_type` and `bolt_version`, determine the right enconding function
  and call it agains `data`
  """
  @spec call_encode(atom(), any(), any()) :: binary() | PackStreamError.t()
  def call_encode(data_type, data, bolt_version)
      when is_integer(bolt_version) and bolt_version in @available_bolt_versions do
    do_call_encode(data_type, data, bolt_version)
  end

  def call_encode(data_type, data, bolt_version) when is_integer(bolt_version) do
    if bolt_version > BoltVersionHelper.last() do
      call_encode(data_type, data, BoltVersionHelper.last())
    else
      raise PackStreamError,
        data_type: data_type,
        data: data,
        bolt_version: bolt_version,
        message: "Unsupported encoder version"
    end
  end

  def call_encode(data_type, data, bolt_version) do
    raise PackStreamError,
      data_type: data_type,
      data: data,
      bolt_version: bolt_version,
      message: "Unsupported encoder version"
  end

  # Check if the encoder for the given bolt version is capable of encoding the given data
  # If it is the case, the encoding function will be called
  # If not, fallback to previous bolt version
  #
  # If encoding function is present in none of the bolt  version, an error will be raised



end
