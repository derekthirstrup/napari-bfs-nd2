# Installation

## Requirements

- Python 3.11–3.13.
- napari 0.8.x for the supported plugin environment.
- A supported Qt backend supplied by the napari installation.

The plugin intentionally does not install a Qt backend. This prevents it from
replacing or conflicting with the backend already selected by napari.

## Install with napari

Once the package is indexed by napari Hub:

1. Start napari.
2. Choose **Plugins > Install/Uninstall Plugins**.
3. Search for `napari-bfs-nd2`.
4. Choose **Install**.
5. Restart napari if **BFS Remote ND2/CZI Reader** is not immediately visible
   under **Plugins**.

## Install with pip

Use the interpreter belonging to the environment that starts napari:

```bash
python -m pip install napari-bfs-nd2
```

If napari is not installed in that environment, install the optional napari
dependency:

```bash
python -m pip install "napari-bfs-nd2[napari]"
```

Then start napari from the same shell:

```bash
napari
```

## Install a downloaded wheel

```bash
python -m pip install /path/to/napari_bfs_nd2-0.3.0-py3-none-any.whl
```

Windows PowerShell example:

```powershell
python -m pip install "C:\Downloads\napari_bfs_nd2-0.3.0-py3-none-any.whl"
```

## Install from source

```bash
git clone https://github.com/derekthirstrup/napari-bfs-nd2.git
cd napari-bfs-nd2
python -m pip install .
```

For an editable development installation:

```bash
python -m pip install -e ".[test,docs]"
```

## Verify plugin discovery

```bash
python -m npe2 list
```

The output should contain `napari-bfs-nd2`. You can also validate the packaged
manifest directly:

```bash
python -m npe2 validate src/napari_bfs_nd2/napari.yaml
```

## Upgrade

```bash
python -m pip install --upgrade napari-bfs-nd2
```

Restart napari after upgrading so the running process reloads the plugin
manifest and Python modules.

## Uninstall

Use **Plugins > Install/Uninstall Plugins** in napari, or run:

```bash
python -m pip uninstall napari-bfs-nd2
```

If napari still shows the old menu after removal, close all napari processes
and start a new one.
