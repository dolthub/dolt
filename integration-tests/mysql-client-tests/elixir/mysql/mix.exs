defmodule MySQLOTP.MixProject do
  use Mix.Project

  def project do
    [
      app: :mysql_otp_test,
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
      mod: {MySQLOTP.Application, []}
    ]
  end

  defp releases do
    [
      mysql_otp: [
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
      {:mysql, "~> 1.9.0"},
      {:burrito, "~> 1.4.0"}
    ]
  end
end

