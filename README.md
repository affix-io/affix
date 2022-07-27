<p align=center> Affix </p>

---

**Affix is a Medical Dataset versioning system, built in go, on IPFS, for the distributed web**

Affix uses advanced language modeling and a network of scientific data engineering pipelines to produce datasets that accelerate the probability of discoveries connected to genes, proteins, pathways, RNA, molecular sequences and other biochemicals. Affix detects, extracts and validates hidden relationships between objects can quicken the pace of discovery in space biosciences.

Note that the binary is still under development and is not stable

## Building From Source

To build affix you'll need the [go programming language](https://golang.org/dl/) on your machine.

````shell
$ git clone https://github.com/affix-io/affix
$ cd affix
$ make install

```shell
$ affix help
````

and see help output.

### Building on Windows

To start, make sure that you have enabled [Developer Mode](https://docs.microsoft.com/en-us/windows/uwp/get-started/enable-your-device-for-development). A library that we depend on needs it enabled in order to properly handle symlinks. If not done, you'll likely get the error message "A required privilege is not held by the client".

You should not need to Run As Administrator to build or run `affix`. We do not recommend using administrator to run `affix`.

#### Shell

For your shell, we recommend using [msys2](https://www.msys2.org/). Other shells, such as `cmd`, `Powershell`, or `cygwin` may also be usable, but `msys2` makes it easy to install our required dependencies. IPFS also recommends `msys2`, and `affix` is built on top of IPFS.

#### Dependencies

Building depends upon having `git` and `make` installed. If using `msys2`, you can easily install these by using the package manager "pacman". In a shell, type:

```shell
pacman -S git make
```

Assuming you've also installed `go` using the official Windows installer linked above, you will also need to add `go` to your `PATH` by modifying your environment variable. See the next section on "Environment variables" for more information.

Due to how msys2 treats the `PATH` variable, you also need to add a new environment variable `MSYS2_PATH_TYPE`, with the value `inherit`, using the same procedure.

Once these steps are complete, proceed to <a href="#building">building</a>.

### Building on Rasberry PI

On a Raspberry PI, you'll need to increase your swap file size in order to build. Normal desktop and server linux OSes should be fine to proceed to <a href="#building">building</a>.

One symptom of having not enough swap space is the `go install` command producing an error message ending with:

```
link: signal: killed
```

To increase your swapfile size, first turn off the swapfile:

```
sudo dphys-swapfile swapoff
```

Then edit `/etc/dphys-swapfile` as root and set `CONF_SWAPSIZE` to 1024.

Finally turn on the swapfile again:

```
sudo dphys-swapfile swapon
```

Otherwise linux machines with reduced memory will have other ways to increase their swap file sizes. Check documentation for your particular machine.

## Packages

affix is comprised of many specialized packages. Below you will find a summary of each package.
