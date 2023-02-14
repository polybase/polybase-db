set -xeio pipefail

ns="ns$RANDOM"

echo "Namespace: $ns"

function test_public() {
    schema="@public collection User { id: string; name: string; constructor (name: string) { this.id = name; this.name = name; } } @public collection Account { id: string; user: User; info: { id: string; }; constructor (id: string, user: User) { this.id = id; this.user = user; this.info = { id: id }; } noop() {} }"

    user_col="$ns/User"
    user_col_escaped="$ns%2FUser"
    http POST :8080/v0/collections/Collection/records args:='["'"$user_col"'", "'"$schema"'"]' "X-Polybase-Signature: pk=$PK_1,sig=0x043705a6972c80f44ac338c3bf8917899773eb2e119fc52da13c02f98da6abe33a31aa1d600f753d01e3848d57b381395ad2b43a58c40ee3d951036d9345683600,t=$(date +%s),v=1,h=deadbeef"

    account_col="$ns/Account"
    account_col_escaped="$ns%2FAccount"
    http POST :8080/v0/collections/Collection/records args:='["'"$account_col"'", "'"$schema"'"]' "X-Polybase-Signature: pk=$PK_1,sig=0x043705a6972c80f44ac338c3bf8917899773eb2e119fc52da13c02f98da6abe33a31aa1d600f753d01e3848d57b381395ad2b43a58c40ee3d951036d9345683600,t=$(date +%s),v=1,h=deadbeef"

    http POST ":8080/v0/collections/$user_col_escaped/records" args:='["john"]'
    http POST ":8080/v0/collections/$account_col_escaped/records" args:='["johnacc", { "collectionId": "'"$user_col"'", "id": "john" }]'
    http POST ":8080/v0/collections/$account_col_escaped/records" args:='["johnacc2", { "collectionId": "'"$user_col"'", "id": "john" }]'

    http POST ":8080/v0/collections/$account_col_escaped/records/johnacc/call/noop" args:='[]'

    http GET ":8080/v0/collections/$account_col_escaped/records/johnacc"

    http GET :8080/v0/collections/$account_col_escaped/records where=='{"id": "johnacc"}'

    http GET :8080/v0/collections/$account_col_escaped/records where=='{"info.id": "johnacc"}'

    # TODO: fix this, sorting shouldn't work here if info.id is not filtered by equality (index mismatch).
    # http GET :8080/v0/collections/$account_col_escaped/records sort=='[["info.id", "asc"], ["id", "desc"]]'
}

function test_private() {
    schema="collection User { id: string; @read @delegate pk: PublicKey; constructor (id: string) { this.id = id; this.pk = ctx.publicKey; } } collection Account { id: string; @read user: User; constructor (id: string, user: User) { this.id = id; this.user = user; } @call(user) noop() {} }"

    user_col="$ns/User"
    user_col_escaped="$ns%2FUser"
    http POST :8080/v0/collections/Collection/records args:='["'"$user_col"'", "'"$schema"'"]' "X-Polybase-Signature: pk=$PK_1,sig=0x043705a6972c80f44ac338c3bf8917899773eb2e119fc52da13c02f98da6abe33a31aa1d600f753d01e3848d57b381395ad2b43a58c40ee3d951036d9345683600,t=$(date +%s),v=1,h=deadbeef"

    account_col="$ns/Account"
    account_col_escaped="$ns%2FAccount"
    http POST :8080/v0/collections/Collection/records args:='["'"$account_col"'", "'"$schema"'"]' "X-Polybase-Signature: pk=$PK_1,sig=0x043705a6972c80f44ac338c3bf8917899773eb2e119fc52da13c02f98da6abe33a31aa1d600f753d01e3848d57b381395ad2b43a58c40ee3d951036d9345683600,t=$(date +%s),v=1,h=deadbeef"

    http POST ":8080/v0/collections/$user_col_escaped/records" args:='["john"]' "X-Polybase-Signature: pk=$PK_1,sig=0x043705a6972c80f44ac338c3bf8917899773eb2e119fc52da13c02f98da6abe33a31aa1d600f753d01e3848d57b381395ad2b43a58c40ee3d951036d9345683600,t=$(date +%s),v=1,h=deadbeef"

    http POST ":8080/v0/collections/$account_col_escaped/records" args:='["johnacc", { "collectionId": "'"$user_col"'", "id": "john" }]' "X-Polybase-Signature: pk=$PK_1,sig=0x043705a6972c80f44ac338c3bf8917899773eb2e119fc52da13c02f98da6abe33a31aa1d600f753d01e3848d57b381395ad2b43a58c40ee3d951036d9345683600,t=$(date +%s),v=1,h=deadbeef"

    http POST ":8080/v0/collections/$account_col_escaped/records/johnacc/call/noop" args:='[]' "X-Polybase-Signature: pk=$PK_1,sig=0x043705a6972c80f44ac338c3bf8917899773eb2e119fc52da13c02f98da6abe33a31aa1d600f753d01e3848d57b381395ad2b43a58c40ee3d951036d9345683600,t=$(date +%s),v=1,h=deadbeef"
}

test_public
# test_private
