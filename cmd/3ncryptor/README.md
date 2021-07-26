# Building:
make build-encryptor

this will put a binary in the `bin` folder called `3ncryptor`

Make sure to copy the binary to one of the px nodes.
# Pre-requisites:

First lets get the admin token from the kubernetes secrt
`kubectl -n kubes-system get secret px-admin-token -o yaml`

Now lets decode the actual auth-token and store it on a `token` variable for later use
`token=echo "<token>" | base64 -d`

We can also go take a look at our pvc specification for the encryption secret.
You will need this during the encryption step.

## Usage:
### Step 1. Snapshot the volumes we want to snapshot. 

We can either specify a namespace or volume_ids which is a comma-seperated string.
These are valid for all 3 commands.

entire namespace:
`3ncryptor snap --namespace kube-system --auth_token $token`

specific volumes:
`3ncryptor snap --volume_ids="vol1,vol2" --auth_token $token`

### Step 2. Encrypt the actual volumes.
In this step we'll go through a couple steps to encrypt the actual volume.
Also you'll see a --secret parameter which requires the encryption secret.

The steps this process walks through are as followed:
- Look for snapshots of the requested volume
- creating a temporary encrypted volume
- Attaching / Mounting both the snapshot and the temp volume.
- Rsync all the data between the 2 volumes with checksum enabled
- delete the original volume
- clone the temp volume into the original volume name.
- delete the temp volume.

We now have an encrypted version of the original volume and an unencrypted snapshot of the original volume.

entire namespace:
`3ncryptor encrypt --namespace kube-system --auth_token $token --secret test-key`

specific volumes:
`3ncryptor encrypt volume_ids="vol1,vol2" --auth_token $token --secret test-key`

### Rollback.
The rollback feature allows for original volumes (if we might have encrypted it already) or snapshots of the original volume.
In both cases we'll delete the original volume name (either encrypted or not) and we'll clone the snapshot back to get to the original state.

entire namespace:
`3ncryptor rollback --namespace kube-system --auth_token $token`

specific volumes:
`3ncryptor rollback volume_ids="vol1,vol2" --auth_token $token`
