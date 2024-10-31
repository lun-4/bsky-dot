import Config

# We don't run a server during test. If one is required,
# you can enable the server option below.
config :dot, DotRunnerWeb.Endpoint,
  http: [ip: {127, 0, 0, 1}, port: 4002],
  secret_key_base: "i1gz8666bG4RZdR/vkaU0k10XubwmRw10/J16J3Hy+lT8/fPfzb26777kll2q3Xx",
  server: false

# Print only warnings and errors during test
config :logger, level: :warning

# Initialize plugs at runtime for faster test compilation
config :phoenix, :plug_init_mode, :runtime
