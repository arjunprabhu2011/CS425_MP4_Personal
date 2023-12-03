ifeq (, $(shell which cargo))
$(warning No `cargo` in path, consider installing Cargo with:)
$(warning - `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`)
$(warning - Or visit https://www.rust-lang.org/tools/install for more on installing Rust.)
$(error Unable to invoke cargo)
endif

RELEASE_DIR = target/release
MP1 = mp1
MP2 = mp2
MP3 = mp3
MP4 = mp4

.PHONY: release
release: clean-exe
	cargo build --release
	mv $(RELEASE_DIR)/$(MP1)-code $(MP1)
	mv $(RELEASE_DIR)/$(MP2)-code $(MP2)
	mv $(RELEASE_DIR)/$(MP3)-code $(MP3)
	mv $(RELEASE_DIR)/$(MP4)-code $(MP4)

clean-exe:
	rm -f $(MP1)
	rm -f $(MP2)
	rm -f $(MP3)
	rm -f $(MP4)

clean: clean-exe
	cargo clean
