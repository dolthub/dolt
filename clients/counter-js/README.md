# counter-js

counter-js uses noms to read and write a simple incrementing counter


## Getting Started

```
cd ../server
go build
./server -ldb=/tmp/counter-js &
cd ../../clients/counter-js
npm install
npm run build
node dist/main.js http://localhost:8000/:counter-js
```
