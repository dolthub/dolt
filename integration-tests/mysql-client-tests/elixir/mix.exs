defmodule Simple.MixProject do
  use Mix.Project

  def project do
    [
      app: :simple,
      version: "0.1.0",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:myxql, "~> 0.5.0"},
    ]
  end
end
