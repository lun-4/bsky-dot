defmodule DotRunnerWeb.InferController do
  use DotRunnerWeb, :controller

  def sentiment(conn, %{"text" => text}) do
    output = Nx.Serving.batched_run(Dot.Serving, text)

    conn
    |> json(%{
      "output" => output
    })
  end
end
