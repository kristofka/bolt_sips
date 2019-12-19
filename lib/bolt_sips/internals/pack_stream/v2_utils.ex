defmodule Bolt.Sips.Internals.PackStream.V2Utils do
  alias Bolt.Sips.Types.Duration

  defmacro __using__(_options) do
    quote do
      import unquote(__MODULE__)

      @spec day_time(Time.t()) :: integer()
      defp day_time(time) do
        Time.diff(time, ~T[00:00:00.000], :nanosecond)
      end

      @spec decompose_datetime(Calendar.naive_datetime()) :: [integer()]
      defp decompose_datetime(%NaiveDateTime{} = datetime) do
        datetime_micros = NaiveDateTime.diff(datetime, ~N[1970-01-01 00:00:00.000], :microsecond)

        seconds = div(datetime_micros, 1_000_000)
        nanoseconds = rem(datetime_micros, 1_000_000) * 1_000

        [seconds, nanoseconds]
      end

      @spec compact_duration(Duration.t()) :: [integer()]
      defp compact_duration(%Duration{} = duration) do
        months = 12 * duration.years + duration.months
        days = 7 * duration.weeks + duration.days
        seconds = 3600 * duration.hours + 60 * duration.minutes + duration.seconds

        [months, days, seconds, duration.nanoseconds]
      end
    end
  end
end
