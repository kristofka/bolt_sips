use Mix.Config

config :mix_test_watch,
  clear: true

level = :debug
#level =
#  if System.get_env("DEBUG") do
#    :debug
#  else
#    :info
#  end

config :bolt_sips,
  log: true,
  log_hex: true

config :logger, :console,
  level: level,
  format: "$date $time [$level] $metadata$message\n"

# Bolt for neo4j
config :bolt_sips, Bolt,
       hostname: 'localhost',
       basic_auth: [username: "neo4j", password: "#####"],
       port: 7688,
       pool_size: 5,
       max_overflow: 1