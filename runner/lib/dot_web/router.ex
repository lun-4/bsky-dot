defmodule DotRunnerWeb.Router do
  use DotRunnerWeb, :router

  pipeline :api do
    plug :accepts, ["json"]
  end

  scope "/v1", DotRunnerWeb do
    pipe_through :api
    get("/models", InferController, :models)
  end

  scope "/api/v1", DotRunnerWeb do
    pipe_through :api
    post("/sentiment", InferController, :sentiment)
  end

  # Enable LiveDashboard in development
  if Application.compile_env(:dot, :dev_routes) do
    # If you want to use the LiveDashboard in production, you should put
    # it behind authentication and allow only admins to access it.
    # If your application does not have an admins-only section yet,
    # you can use Plug.BasicAuth to set up some basic authentication
    # as long as you are also using SSL (which you should anyway).
    import Phoenix.LiveDashboard.Router

    scope "/dev" do
      pipe_through [:fetch_session, :protect_from_forgery]

      live_dashboard "/dashboard", metrics: DotRunnerWeb.Telemetry
    end
  end
end