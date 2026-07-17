# Telegram TGS renderer

The following unmodified runtime files are served by Telegram's public NFT pages and are vendored here so collectible animations work reliably inside the Mini App:

- `tgsticker.js` — source: `https://telegram.org/js/tgsticker.js?32`; the worker URL was made relative to the Mini App base URL and player cleanup now also releases its worker item.
- `tgsticker-worker.js` — source: `https://telegram.org/js/tgsticker-worker.js?14`; pending loads can be cancelled safely when a card leaves the viewport.
- `rlottie-wasm.js` — source: `https://telegram.org/js/rlottie-wasm.js`.
- `rlottie-wasm.wasm` — source: `https://telegram.org/js/rlottie-wasm.wasm`.
- `pako-inflate.min.js` — source: `https://telegram.org/js/pako-inflate.min.js`.

The media itself is not copied into this repository. At runtime it is obtained from the public `t.me/nft/...` page associated with the catalog item.
