defmodule EJDB2.MixProject do
  use Mix.Project

  def project do
    [
      app: :ejdb2,
      version: "0.1.0",
      elixir: "~> 1.10",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:websockex, "~> 0.4.3"},
      {:jason, "~> 1.2"},
      {:erlexec, "~> 1.0", only: :test}
    ]
  end
end
