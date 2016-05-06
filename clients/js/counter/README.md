# js/counter

counter uses noms/js to read and write a simple incrementing counter.


## Getting Started

```
cd ../server
go build
./server ldb:/tmp/noms &
cd ../../clients/js/counter
npm install
npm run build
node . http://localhost:8000:counter
```
