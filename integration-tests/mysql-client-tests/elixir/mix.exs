defmodule Simple.MixProject do
  use Mix.Project

  def project do
    [
      app: :simple,
      version: "0.1.0",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      releases: releases()
    ]
  end

  def application do
    [
      extra_applications: [:logger, :crypto, :public_key, :ssl],
      mod: {Simple.Application, []}
    ]
  end

  defp releases do
    [
      simple: [
        steps: [:assemble, &Burrito.wrap/1],
        burrito: [
          targets: [
            linux: [os: :linux, cpu: :x86_64]
          ],
          no_native_archivers: true
        ]
      ]
    ]
  end

  defp deps do
    [
      {:myxql, "~> 0.5.0"},
      {:burrito, "~> 1.4.0"}
    ]
  end
end
