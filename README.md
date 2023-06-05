# Polybase (Rust)

The decentralized database for web3. 


## Get Started

### Single Node

Start a single node running using the following command:

```sh
cd polybase && cargo run
```

On first run, this will create a key at the `ROOT_DIR` (defaults to `~/.polybase`).

### Multiple Nodes

To run multiple nodes on the same machine, you will need to provide additional configuration parameters to prevent conflicts. You should 
run an odd number of nodes, otherwise the deadlock can occur (as no majority can be obtained). If you change the secret key for any of the nodes,
you will need to also update the peer also (peer is derived from secret key).

**Node 1:**

```sh
cargo run -- --secret-key=0x7facdbac164c18d2e8fdd12cbc9e4f44600459243bed1cf0398caa83f96785d5 --rpc-laddr=0.0.0.0:8081 --network-laddr=/ip4/0.0.0.0/tcp/5001 --peers=12D3KooWBbUxBUCsHfVWGWKKh4SdVuETpemY74HFi8M2HFXBC79s,12D3KooWKdxNkJd3wwLYWGFXvvr4hn8VDzfxptUVQJhM4AERANLP,12D3KooWHA54FotdKaVb8g8qs6sfAYAiUYsXbjYNRunHLn3GAebJ --dial-addr=/ip4/127.0.0.1/tcp/5001,/ip4/127.0.0.1/tcp/5002,/ip4/127.0.0.1/tcp/5003 --root-dir=~/.polybase/n1 --log-level=DEBUG
```

**Node 2:**

```sh
cargo run -- --secret-key=0x5b3b10ee95322e85b2d9cfb527095e51fb853d0b5961a321c8eb29f1cc770e09 --rpc-laddr=0.0.0.0:8082 --network-laddr=/ip4/0.0.0.0/tcp/5002 --peers=12D3KooWBbUxBUCsHfVWGWKKh4SdVuETpemY74HFi8M2HFXBC79s,12D3KooWKdxNkJd3wwLYWGFXvvr4hn8VDzfxptUVQJhM4AERANLP,12D3KooWHA54FotdKaVb8g8qs6sfAYAiUYsXbjYNRunHLn3GAebJ --dial-addr=/ip4/127.0.0.1/tcp/5001,/ip4/127.0.0.1/tcp/5002,/ip4/127.0.0.1/tcp/5003 --root-dir=~/.polybase/n2 --log-level=DEBUG
```

**Node 3:**

```sh
cargo run -- --secret-key=0xf3982b58c55dcfce671c882c00cc97f58ea5c7fc5f37f4a76e566da425d4e162 --rpc-laddr=0.0.0.0:8083 --network-laddr=/ip4/0.0.0.0/tcp/5003 --peers=12D3KooWBbUxBUCsHfVWGWKKh4SdVuETpemY74HFi8M2HFXBC79s,12D3KooWKdxNkJd3wwLYWGFXvvr4hn8VDzfxptUVQJhM4AERANLP,12D3KooWHA54FotdKaVb8g8qs6sfAYAiUYsXbjYNRunHLn3GAebJ --dial-addr=/ip4/127.0.0.1/tcp/5001,/ip4/127.0.0.1/tcp/5002,/ip4/127.0.0.1/tcp/5003 --root-dir=~/.polybase/n3 --log-level=DEBUG
```

## Generating Keys

If you want to a new generate key / peer, you can use the following command. Make sure to update the 

```sh
cargo run -- generate_key
```

## Limitations

 * Peers list cannot be updated and must be provided at start up.


## Test

```sh
cargo test
```

## API

API server runs on port 8080 by default:

**[GET] /** - server index

**[GET] /v0/status** - provides details on consensus status

**[GET] /v0/health** - returns 200 if the node is healthy, or an error otherwise

**[POST] /v0/collections/[colId]/records** - create a new record for a collection (calls the constructor fn with provided arguments)

**[GET] /v0/collections/[colId]/records?since=&waitFor=&where=&after=&before=&limit=** - list collection records

**[GET] /v0/collections/[colId]/records/[recId]?since=&waitFor=** - get a specific record in collection

**[POST] /v0/collections/[colId]/records/[recId]/call/[function]** - call a function on a collection (via solid tx)



### Creating a collection

To create a collection (aka collection/table), you must send a POST request to `/v0/collections/Collection` with the code for your collection. 

```graphql
collection CollectionName {
  id: string;
  name: string;

  constructor (id: string, name: string) {
    this.id = id;
    this.name = name;
  }

  updateName (name: string) {
    this.name = name;
  }
}
```

### Authentication

Authentication is optional.
You can sign request bodies with your private key.
A signature is valid for 5 minutes.

Example, using our `auth` package:
```go
signature, err := auth.Sign(privateKey, &auth.Message{
  Timestamp: strconv.FormatInt(time.Now().Unix(), 10),
  Body:      []byte(`{ "name": "John" }`),
})
```

Example, using web3:
```javascript
const body = `{ "name": "John" }`;
const timestamp = Math.floor(new Date().getTime() / 1000);
const account = (await web3.eth.requestAccounts())[0];
const signature = await web3.eth.sign_personal(hash, account);
```

From variables in the web3.js example above, you can build a header like this:
```javascript
const headers = {
  "X--Signature": `${publicKey ? `pk=${publicKey},` : ""}sig=${signature},t=${timestamp},v0=1,h=eth-personal-sign`,
};
```