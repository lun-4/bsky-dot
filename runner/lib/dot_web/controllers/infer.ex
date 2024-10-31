defmodule DotRunnerWeb.InferController do
  use DotRunnerWeb, :controller

  def models(conn, _params) do
    conn
    |> json(%{
      object: "list",
      data: [
        %{
          id: "cardiffnlp/twitter-roberta-base-sentiment-latest"
        }
      ]
    })
  end

  def sentiment(conn, %{"text" => text}) do
    output = Nx.Serving.batched_run(Dot.Serving, text)

    conn
    |> json(%{
      "output" => output
    })
  end
end
