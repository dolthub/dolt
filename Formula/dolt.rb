class Dolt < Formula
  desc "Dolt - It's git for data"
  homepage "https://github.com/liquidata-inc/dolt"
  url "https://github.com/liquidata-inc/dolt"
  sha256 "2c13dfcf594f5c52780ccc97e932bcfd3128b07ff43018e1622a066b4ff36a3a"

  bottle :unneeded

  def install
    system 'cd go'
    system 'go install ./cmd/dolt'
    system 'go install ./cmd/git-dolt'
    system 'go install ./cmd/git-dolt-smudge'
  end

  test do
    system "#{bin}/dolt", '--version'
  end
end