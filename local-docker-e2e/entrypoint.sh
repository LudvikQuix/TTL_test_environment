#!/bin/sh
# Put the read-only bind-mounted quixstreams working tree on PYTHONPATH so
# `import quixstreams` resolves to the live tree at /quix-streams (deps are
# baked into the image). This is the read-only-mount-safe equivalent of
# `pip install -e /quix-streams --no-deps` (see Dockerfile note / spec 5.3).
set -eu

export PYTHONPATH="/quix-streams${PYTHONPATH:+:$PYTHONPATH}"

# Fail fast + loud if the live tree cannot be imported. Per spec R-3 a broken /
# mid-edit library tree must surface as an infra error (container exits != 0),
# never as a false assertion pass. This line goes to stderr, keeping the
# inspector's stdout pure JSON.
python -c "import quixstreams,sys;sys.stderr.write('[entrypoint] quixstreams %s from %s\n'%(getattr(quixstreams,'__version__','?'),quixstreams.__file__))"

exec "$@"
