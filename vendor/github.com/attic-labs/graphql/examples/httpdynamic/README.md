Basically, if we have `data.json` like this:

    [
      { "id": "1", "name": "Dan" },
      { "id": "2", "name": "Lee" },
      { "id": "3", "name": "Nick" }
    ]

... and `go run main.go`, we can query records:

	$ curl -g 'http://localhost:8080/graphql?query={user(name:"Dan"){id}}'
	{"data":{"user":{"id":"1"}}}

... now let's give Dan a surname:

    [
      { "id": "1", "name": "Dan", "surname": "Jones" },
      { "id": "2", "name": "Lee" },
      { "id": "3", "name": "Nick" }
    ]

... and kick the server:

    kill -SIGUSR1 52114

And ask for Dan's surname:

	$ curl -g 'http://localhost:8080/graphql?query={user(name:"Dan"){id,surname}}'
	{"data":{"user":{"id":"1","surname":"Jones"}}}

... or ask Jones's name and ID:

    $ curl -g 'http://localhost:8080/graphql?query={user(surname:"Jones"){id,name}}'
    {"data":{"user":{"id":"1","name":"Dan"}}}

If you look at `main.go`, the file is not field-aware. That is, all it knows is
how to work with `[]map[string]string` type.

With this, we are not that far from exposing dynamic fields and filters which
fully depend on what we have stored, all without changing our tooling.
