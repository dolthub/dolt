# HR

This is a small command line application that manages a very simple hypothetical hr database.

## Usage

```
go build
./hr --ds /tmp/my-noms::hr add-person 42 Abigail Architect
./hr --ds /tmp/my-noms::hr add-person 43 Samuel "Chief Laser Operator"
./hr --ds /tmp/my-noms::hr list-persons
```
