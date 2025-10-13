// swift-tools-version:5.5
import PackageDescription

let package = Package(
    name: "MariaDBTest",
    platforms: [
        .macOS(.v10_15)
    ],
    dependencies: [
        .package(url: "https://github.com/PerfectlySoft/Perfect-MariaDB.git", from: "3.0.0")
    ],
    targets: [
        .executableTarget(
            name: "MariaDBTest",
            dependencies: [
                .product(name: "MariaDB", package: "Perfect-MariaDB")
            ],
            path: "Sources",
            linkerSettings: [
                .linkedLibrary("mariadb")
            ]
        )
    ]
)

