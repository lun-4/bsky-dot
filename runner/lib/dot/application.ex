defmodule DotRunner.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    {:ok, model_info} =
      Bumblebee.load_model({:hf, "cardiffnlp/twitter-roberta-base-sentiment-latest"})

    tokenizer_path = [:code.priv_dir(:dot), "/cardiffnlp_tokenizer"] |> Enum.join()

    {:ok, tokenizer} =
      Bumblebee.load_tokenizer({:local, tokenizer_path})

    serving =
      Bumblebee.Text.text_classification(model_info, tokenizer,
        top_k: 1,
        compile: [batch_size: 1, sequence_length: 2],
        defn_options: [
          compiler: EXLA,
          cache: Path.join(System.tmp_dir!(), "bumblebee_examples/text_classification")
        ]
      )

    children = [
      DotRunnerWeb.Telemetry,
      {DNSCluster, query: Application.get_env(:dot, :dns_cluster_query) || :ignore},
      {Phoenix.PubSub, name: DotRunner.PubSub},
      {Nx.Serving, serving: serving, name: Dot.Serving, batch_timeout: 100},
      # Start a worker by calling: DotRunner.Worker.start_link(arg)
      # {DotRunner.Worker, arg},
      # Start to serve requests, typically the last entry
      DotRunnerWeb.Endpoint
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: DotRunner.Supervisor]
    Supervisor.start_link(children, opts)
  end

  # Tell Phoenix to update the endpoint configuration
  # whenever the application is updated.
  @impl true
  def config_change(changed, _new, removed) do
    DotRunnerWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end
