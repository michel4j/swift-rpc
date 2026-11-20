from __future__ import annotations

import errno
import hashlib
import os
import random
import re
import socket
import tempfile
import threading
from pathlib import Path
from typing import Generator, Sequence

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows fallback
    fcntl = None

try:
    import msvcrt
except ImportError:  # pragma: no cover - POSIX
    msvcrt = None

# Adjectives (mostly 1-3 syllables)
ADJECTIVES = [
    "able", "acidic", "active", "actual", "adept", "adult", "agile", "alert", "alive", "all",
    "alpha", "ample", "angry", "apt", "artful", "atomic", "august", "aware", "awful", "bad",
    "basic", "basil", "beady", "best", "big", "bitter", "black", "blue", "blunt", "bold",
    "bonny", "bossy", "brave", "brief", "bright", "brisk", "broad", "brown", "bulky", "busy",
    "calm", "canny", "careful", "catchy", "cheap", "chief", "choice", "civil", "clean", "clear",
    "clever", "close", "cold", "comfy", "cool", "coral", "corny", "cosmic", "cosy", "crazy",
    "crisp", "cruel", "cute", "daily", "damp", "dandy", "dark", "dear", "deep", "deft",
    "dense", "dizzy", "dopey", "droll", "dry", "dual", "dull", "dusty", "eager", "early",
    "easy", "edgy", "elastic", "electric", "elfin", "elite", "empty", "epic", "equal", "evil",
    "exact", "extra", "faint", "fair", "fancy", "fast", "fat", "fatal", "fazed", "feral",
    "few", "fierce", "fifth", "fine", "firm", "first", "fit", "fixed", "fizzy", "flaky",
    "flash", "flat", "fleet", "flimsy", "fluent", "fluffy", "fluid", "fond", "foxy", "frail",
    "frank", "free", "fresh", "fretful", "full", "fun", "funny", "fuzzy", "game", "gaudy",
    "gentle", "giant", "giddy", "glad", "glassy", "glib", "gloomy", "gold", "gone", "good",
    "goofy", "grand", "grassy", "gray", "great", "green", "grim", "gross", "grown", "gruff",
    "gummy", "hairy", "halcyon", "hale", "handy", "happy", "hard", "hardy", "harsh", "hasty",
    "hazy", "heady", "hearty", "heavy", "hefty", "heroic", "high", "holy", "honest", "hopeful",
    "hot", "huge", "human", "humble", "humid", "hungry", "husky", "icy", "ideal", "idle",
    "ill", "inner", "intense", "ionic", "iron", "itchy", "jade", "jaded", "jagged", "jazzy",
    "jealous", "jolly", "jovial", "joyful", "juicy", "jumbo", "jumpy", "just", "keen", "khaki",
    "kind", "kingly", "kinky", "known", "kooky", "lacy", "large", "last", "laughing", "lazy",
    "leafy", "lean", "left", "legal", "lemon", "level", "light", "lilac", "limp", "linear",
    "lined", "liquid", "little", "lively", "livid", "lone", "long", "loose", "loud", "lovely",
    "loyal", "lucky", "lumpy", "lunar", "mad", "magic", "main", "major", "many", "marble",
    "mauve", "mealy", "mean", "meaty", "meek", "mellow", "merry", "messy", "metal", "mild",
    "milky", "minor", "minty", "misty", "mixed", "modern", "modest", "moist", "moody", "moral",
    "mossy", "muddy", "mute", "mythic", "naive", "nasty", "native", "navel", "neat", "needy",
    "nervous", "new", "next", "nice", "nifty", "nimble", "noble", "noisy", "normal", "north",
    "nosy", "novel", "null", "numb", "nutty", "oaken", "odd", "oily", "old", "olive",
    "open", "optic", "oral", "orange", "organic", "oval", "pale", "palm", "past", "patient",
    "peaceful", "peachy", "perfect", "perky", "petty", "pink", "plain", "plastic", "playful", "pleasant",
    "plump", "plush", "polite", "poor", "pop", "portly", "posh", "potent", "pretty", "prickly",
    "prime", "prior", "privy", "proud", "pure", "purple", "pushy", "quaint", "quick", "quiet",
    "quirky", "quite", "radio", "rainy", "rapid", "rare", "rash", "raw", "ready", "real",
    "red", "regal", "rich", "right", "rigid", "ripe", "risky", "robust", "rocky", "rogue",
    "rosy", "rough", "round", "royal", "rude", "runny", "rural", "rusty", "sad", "safe",
    "saggy", "saintly", "salty", "sane", "sandy", "sassy", "saucy", "scary", "scenic", "secret",
    "secure", "serene", "shady", "shaggy", "shaky", "shallow", "sharp", "shiny", "short", "showy",
    "shy", "sick", "silent", "silky", "silly", "silver", "simple", "single", "skunky", "sleek",
    "sleepy", "slick", "slight", "slim", "slimy", "slow", "sly", "small", "smart", "smoky",
    "smooth", "smug", "snappy", "sneaky", "snug", "social", "soft", "soggy", "olar", "solid",
    "solo", "sonic", "sore", "sour", "spare", "sparkly", "spicy", "spry", "square", "stable",
    "stale", "starry", "steady", "steep", "sticky", "stiff", "still", "stocky", "stoic", "stormy",
    "stout", "strict", "strong", "stuck", "sturdy", "subtle", "sudden", "sugary", "sunny", "super",
    "sure", "sweet", "swift", "tall", "tame", "tan", "tart", "tasty", "taut", "teal",
    "tender", "tense", "tepid", "testy", "thick", "thin", "tidy", "tight", "tiny", "tired",
    "toasty", "top", "tough", "toxic", "tricky", "trim", "true", "trusty", "twin", "ugly",
    "ultra", "uncut", "undue", "unfair", "unique", "unseen", "unsung", "untrue", "upset", "urban",
    "urgent", "used", "useful", "useless", "vague", "vain", "valid", "vapid", "vast", "velvet",
    "vexed", "vibrant", "violet", "viral", "virtual", "vital", "vivid", "vocal", "vogue", "void",
    "wacky", "warm", "wary", "waste", "weak", "weary", "wee", "weird", "wet", "white",
    "whole", "wicked", "wide", "wild", "windy", "winged", "wired", "wise", "witty", "woozy",
    "wordy", "worn", "worthy", "wrong", "wry", "yellow", "young", "yummy", "zany", "zealous",
    "zesty", "zippy", "zonal"
]

# First Names (mostly 1-3 syllables)
NAMES = [
    "aaron", "abby", "abel", "abraham", "ada", "adam", "adrian", "agnes", "aidan", "aileen",
    "aimee", "alan", "albert", "alden", "alec", "alex", "alfred", "alice", "aline", "allen",
    "alvin", "amanda", "amber", "amos", "amy", "andre", "andy", "angel", "angie", "anita",
    "ann", "anna", "annie", "anthony", "april", "archie", "ariel", "arnold", "art", "arthur",
    "ashley", "audrey", "austin", "avery", "axel", "babs", "bailey", "barbara", "barney", "barry",
    "bart", "basil", "bea", "becky", "bella", "ben", "benny", "bernie", "bert", "bertha",
    "bessie", "betsy", "betty", "beverly", "bill", "billy", "bing", "blair", "blake", "bob",
    "bobby", "bonnie", "boris", "brad", "brenda", "brent", "brett", "brian", "bridget", "britney",
    "brooks", "bruce", "bruno", "bryan", "buffy", "burt", "buster", "butch", "byron", "caleb",
    "calvin", "cameron", "candy", "carl", "carla", "carlo", "carlos", "carol", "carrie", "carson",
    "casey", "casper", "cathy", "cedric", "celia", "cesar", "chad", "chance", "charles", "charlie",
    "chase", "chester", "chloe", "chris", "chuck", "cindy", "claire", "clara", "clare", "clark",
    "claude", "clay", "cleo", "cliff", "clint", "clyde", "cody", "colby", "cole", "colin",
    "conner", "connie", "cooper", "cora", "corey", "corinne", "craig", "cristina", "curt", "cyril",
    "cyrus", "daisy", "dale", "dan", "dana", "daniel", "danny", "dante", "daphne", "darby",
    "darcy", "darin", "dario", "daryl", "dave", "david", "dawn", "dean", "debbie", "deborah",
    "della", "denis", "dennis", "derek", "desmond", "devon", "dewey", "dexter", "diana", "diane",
    "dick", "diego", "dillon", "dina", "dion", "dirk", "dolly", "dom", "dona", "donald",
    "donna", "dora", "doris", "doug", "douglas", "drake", "drew", "duane", "duke", "duncan",
    "dustin", "dwayne", "dwight", "dylan", "earl", "ebony", "eddie", "edgar", "edith", "edmond",
    "edna", "edward", "edwin", "eileen", "elaine", "eli", "elias", "elijah", "eliza", "ella",
    "ellen", "ellie", "elliot", "elmer", "elsa", "elvis", "emil", "emily", "emma", "emmett",
    "enid", "eric", "ernest", "ernie", "esteban", "ester", "ethan", "ethel", "evan", "eve",
    "evelyn", "everett", "fabian", "faith", "fanny", "fay", "felix", "fern", "fidel", "fiona",
    "fletcher", "flo", "flora", "floyd", "flynn", "ford", "forest", "fran", "frances", "francis",
    "frank", "frankie", "fred", "freddie", "frida", "fritz", "gabe", "gabriel", "gail", "gale",
    "gareth", "gary", "gavin", "gemma", "gene", "george", "gerald", "gerard", "gertrude", "gideon",
    "gilbert", "giles", "gill", "ginny", "glen", "glenda", "glenn", "gloria", "gordon", "grace",
    "grady", "graham", "grant", "greg", "greta", "grover", "gus", "gwen", "hailey", "hal",
    "hank", "hanna", "hans", "harley", "harold", "harriet", "harry", "harvey", "hazel", "heath",
    "heather", "hector", "heidi", "helen", "henry", "herb", "herman", "hester", "hilda", "homer",
    "hope", "howard", "hubert", "hugh", "hugo", "ian", "ida", "ignacio", "ike", "ilana",
    "imogen", "ingrid", "ira", "irene", "iris", "irma", "irvin", "irving", "isaac", "isabel",
    "ivan", "ivy", "jack", "jackie", "jacob", "jade", "jake", "james", "jamie", "jan",
    "jane", "janet", "janice", "jared", "jason", "jasper", "jay", "jean", "jeff", "jeffrey",
    "jenna", "jennie", "jenny", "jeremy", "jerry", "jess", "jesse", "jessie", "jill", "jim",
    "jimmy", "joan", "joanna", "jody", "joe", "joel", "joey", "john", "johnny", "jon",
    "jonah", "jonas", "jordan", "jose", "joseph", "josh", "joshua", "josie", "joy", "joyce",
    "juan", "judith", "judy", "julia", "julian", "julie", "julio", "june", "justin", "kara",
    "karen", "kari", "karl", "kate", "kathy", "katie", "kay", "keith", "kelly", "kelvin",
    "ken", "kendra", "kenneth", "kenny", "kent", "kerry", "kevin", "kim", "kirk", "kirsten",
    "kit", "kitty", "klaus", "kris", "kristen", "kristin", "kurt", "kyle", "lana", "lance",
    "larry", "laura", "lauren", "laurie", "lawrence", "leah", "lee", "leila", "lena", "lenny",
    "leo", "leon", "leona", "leonard", "leroy", "les", "leslie", "lester", "lewis", "liam",
    "lila", "lilly", "lily", "lincoln", "linda", "lindsay", "lindsey", "linus", "lionel", "lisa",
    "liz", "liza", "lizzie", "lloyd", "logan", "lois", "lola", "lonnie", "lora", "loren",
    "loretta", "lori", "lorna", "lou", "louie", "louis", "louise", "lucas", "lucia", "lucy",
    "luigi", "luke", "lulu", "luna", "luther", "lydia", "lyle", "lynn", "mabel", "mac",
    "mack", "macy", "maddie", "madge", "maggie", "maisie", "major", "malcolm", "mandy", "manny",
    "mara", "marc", "marcel", "marcus", "marcy", "marge", "margo", "maria", "marie", "marion",
    "mark", "marla", "marsha", "martha", "martin", "mary", "mason", "matt", "maude", "maureen",
    "max", "maxine", "may", "maya", "meg", "megan", "mel", "melba", "melvin", "merle",
    "merrill", "merry", "meryl", "mia", "micah", "michael", "mickey", "mike", "miles", "milton",
    "mimi", "mindy", "minnie", "miranda", "miriam", "mitch", "molly", "mona", "monica", "monte",
    "monty", "morgan", "morris", "morty", "moses", "muriel", "murray", "myra", "myron", "nadia",
    "nan", "nancy", "naomi", "nat", "natalie", "nate", "nathan", "ned", "neil", "nell",
    "nellie", "nelson", "nettie", "neva", "neville", "newt", "nick", "nicky", "nigel", "nina",
    "noah", "noel", "nola", "nora", "norm", "norma", "norman", "norris", "norton", "olive",
    "oliver", "ollie", "omar", "opal", "ora", "orson", "orville", "oscar", "otis", "otto",
    "owen", "ozzy", "pablo", "page", "paige", "pam", "pamela", "pansy", "pat", "patrick",
    "patsy", "patty", "paul", "paula", "pearl", "pedro", "peggy", "penney", "pete", "peter",
    "phil", "philip", "phoebe", "pierce", "pierre", "piper", "pippa", "polly", "preston", "prince",
    "priscilla", "quentin", "quincy", "quinn", "rachel", "ralph", "ramon", "randy", "raoul", "raphael",
    "ray", "raymond", "reba", "rebecca", "reed", "reese", "reggie", "regina", "reid", "rene",
    "renee", "rex", "rhett", "rhoda", "rhonda", "richard", "rick", "ricky", "rico", "riley",
    "rita", "rob", "robbie", "robert", "robin", "rocco", "rocky", "rod", "rodney", "roger",
    "roland", "roman", "romeo", "ron", "ronald", "ronnie", "rory", "rosa", "roscoe", "rose",
    "rosie", "ross", "rowan", "roxanne", "roy", "ruben", "ruby", "rudy", "rufus", "rupert",
    "russ", "russell", "rusty", "ruth", "ryan", "sabrina", "sadie", "sal", "sally", "sam",
    "sammy", "samuel", "sandra", "sandy", "sara", "sarah", "sasha", "saul", "scott", "sean",
    "sebastian", "selma", "serena", "seth", "seymour", "shane", "shannon", "shari", "sharon", "shawn",
    "sheila", "shelby", "sheldon", "shelly", "sherman", "sherry", "shirley", "sid", "sidney", "silas",
    "simon", "simone", "skip", "skye", "sly", "sofia", "sol", "sonia", "sonny", "sophia",
    "sophie", "spencer", "stacy", "stan", "stanley", "stella", "steph", "stephen", "steve", "steven",
    "stevie", "stewart", "stu", "stuart", "sue", "summer", "susan", "susie", "suzanne", "suzy",
    "sydney", "sylvia", "tabitha", "tad", "tami", "tanya", "tara", "tasha", "taylor", "ted",
    "teddy", "terence", "teresa", "terry", "tess", "tessa", "theo", "thomas", "tim", "timmy",
    "timothy", "tina", "toby", "todd", "tom", "tommy", "tony", "tory", "tracey", "tracy",
    "travis", "trent", "trevor", "tricia", "trixie", "troy", "trudy", "tyler", "tyrone", "uma",
    "val", "valerie", "van", "vanessa", "velma", "vera", "vernon", "veronica", "vic", "vicki",
    "victor", "victoria", "vince", "vincent", "innie", "viola", "violet", "virgil", "vivian", "vlad",
    "wade", "waldo", "walker", "wally", "walt", "walter", "wanda", "ward", "warren", "wayne",
    "wendy", "wes", "wesley", "whitney", "wilbur", "will", "willa", "william", "willie", "wilma",
    "wilson", "winifred", "winnie", "winston", "wolf", "woody", "wyatt", "xavier", "yetta", "yogi",
    "yolanda", "york", "yvette", "yvonne", "zach", "zachary", "zack", "zelda", "zeke", "zoe"
]


class RandomGenerator:
    """
    A name generator, guaranteed to be unique when generating less than 65,536 names
    """
    def __init__(self, separator: str = '-', seed: int | bytes | str | None = None):
        self._rng = random.Random(seed)
        self.separator = separator
        self.names = NAMES[:]
        self.adjectives = ADJECTIVES[:]
        self.total_names = len(self.names) * len(self.adjectives)
        self._rng.shuffle(self.names)
        self._rng.shuffle(self.adjectives)
        self.name_generator = self.fisher_yates(self.total_names, rng=self._rng)

    @staticmethod
    def fisher_yates(size: int, rng: random.Random | None = None) -> Generator[int, None, None]:
        """
        A generator that yields unique random integers from 0 to size-1.
        
        This implements a sparse Fisher-Yates shuffle, allowing for O(k) memory usage
        to generate k items, rather than O(size).
        """
        # Track swapped elements: index -> value
        # If an index is missing, its value is the index itself.
        rand = rng if rng is not None else random
        swaps = {}
        
        for i in reversed(range(size)):
            j = rand.randrange(i + 1)
            yield swaps.get(j, j)       # Yield the value at index j
            
            # Move the value from index i to index j and remove it from swaps
            # as it is now out of the active range
            swaps[j] = swaps.get(i, i)
            swaps.pop(i, None)

    def generate_name(self) -> str:
        """
        Generates a random name in the format: adjective{separator}first_name.
        :return: A random string (e.g., 'happy-darwin').
        """
        try:
            index = next(self.name_generator)
        except StopIteration:
            self.name_generator = self.fisher_yates(self.total_names, rng=self._rng)
            index = next(self.name_generator)

        adj_index, name_index = divmod(index, len(self.names))
        adj = self.adjectives[adj_index]
        name = self.names[name_index]
        return f"{adj}{self.separator}{name}"

    def generate_names(self, count: int = 1) -> list[str]:
        """
        Generates a set of unique random names.

        :param count: The number of unique names to generate
        :return: list of random names
        """

        unique_names = set()

        while len(unique_names) < count:
            unique_names.add(self.generate_name())

        return list(unique_names)


_INPROC_LOCK = threading.Lock()
_INPROC_ACTIVE: set[Path] = set()


def default_host_name() -> str:
    """Return the uppercase short hostname used in worker identities."""
    return socket.getfqdn().split('.')[0].upper()


def _normalize_backend(backend: str) -> str:
    normalized = backend.strip().lower()
    normalized = normalized.replace('://*:', '://localhost:')
    normalized = normalized.replace('://127.0.0.1:', '://localhost:')
    normalized = normalized.replace('://[::1]:', '://localhost:')
    return normalized


def default_lock_dir() -> Path:
    """Return the ephemeral directory for live worker identity lock files."""
    runtime_dir = os.environ.get('XDG_RUNTIME_DIR')
    if runtime_dir:
        return Path(runtime_dir) / 'szrpc' / 'locks'
    uid = os.getuid() if hasattr(os, 'getuid') else 'user'
    return Path(tempfile.gettempdir()) / f'szrpc-{uid}' / 'locks'


def _try_lock_fd(fd: int) -> bool:
    if fcntl is not None:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except OSError as exc:
            if exc.errno in (errno.EACCES, errno.EAGAIN, errno.EWOULDBLOCK):
                return False
            raise
    if msvcrt is not None:  # pragma: no cover - Windows
        try:
            os.lseek(fd, 0, os.SEEK_SET)
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            return True
        except OSError:
            return False
    return True  # pragma: no cover


def _unlock_fd(fd: int) -> None:
    if fcntl is not None:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        except OSError:
            pass
    elif msvcrt is not None:  # pragma: no cover - Windows
        try:
            os.lseek(fd, 0, os.SEEK_SET)
            msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
        except OSError:
            pass


class WorkerIdentityLease:
    """Holds a deterministic, OS-locked worker identity for the lifetime of a Worker.

    Names are generated deterministically from ``(host, service_name, backend, methods)``
    using ``RandomGenerator``, while an in-process set + OS advisory file lock (``flock``)
    ensures concurrent threads and processes targeting the same broker never collide.
    Locks are released automatically by the OS kernel if a worker crashes.
    """

    def __init__(self, worker_id: bytes, fd: int, lock_path: Path):
        self.worker_id = worker_id
        self._fd: int | None = fd
        self._lock_path = lock_path
        self._released = False

    @property
    def name(self) -> str:
        return self.worker_id.decode('utf-8')

    @classmethod
    def acquire(
        cls,
        service_name: str,
        backend: str,
        methods: Sequence[str] = (),
        *,
        host: str | None = None,
        lock_dir: str | Path | None = None,
    ) -> 'WorkerIdentityLease':
        resolved_host = (host or default_host_name()).upper()
        norm_backend = _normalize_backend(backend)
        sorted_methods = tuple(sorted(set(methods)))

        broker_scope = f"{service_name}\x00{norm_backend}"
        broker_dir_name = hashlib.sha256(broker_scope.encode('utf-8')).hexdigest()[:16]

        seed_key = f"{resolved_host}\x00{service_name}\x00{norm_backend}\x00{','.join(sorted_methods)}"
        seed = int.from_bytes(hashlib.sha256(seed_key.encode('utf-8')).digest(), 'big')
        gen = RandomGenerator(seed=seed)

        root = Path(lock_dir) if lock_dir is not None else default_lock_dir()
        ns_dir = root / broker_dir_name
        ns_dir.mkdir(parents=True, exist_ok=True)

        for _ in range(gen.total_names):
            candidate = f"{gen.generate_name()}@{resolved_host}"
            safe_name = re.sub(r'[^a-zA-Z0-9._@-]+', '_', candidate)
            lock_path = (ns_dir / f"{safe_name}.lock").resolve()

            with _INPROC_LOCK:
                if lock_path in _INPROC_ACTIVE:
                    continue
                _INPROC_ACTIVE.add(lock_path)

            fd: int | None = None
            try:
                fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
                if not _try_lock_fd(fd):
                    os.close(fd)
                    fd = None
                    with _INPROC_LOCK:
                        _INPROC_ACTIVE.discard(lock_path)
                    continue
                try:
                    os.ftruncate(fd, 0)
                    os.lseek(fd, 0, os.SEEK_SET)
                    os.write(fd, f"{os.getpid()}\n".encode('ascii'))
                except OSError:
                    pass
                return cls(candidate.encode('utf-8'), fd, lock_path)
            except BaseException:
                if fd is not None:
                    try:
                        os.close(fd)
                    except OSError:
                        pass
                with _INPROC_LOCK:
                    _INPROC_ACTIVE.discard(lock_path)
                raise

        raise RuntimeError(f'Exhausted all {gen.total_names} worker identity slots for {service_name}')

    def release(self) -> None:
        with _INPROC_LOCK:
            if self._released:
                return
            self._released = True
            fd = self._fd
            self._fd = None
            _INPROC_ACTIVE.discard(self._lock_path)

        if fd is not None:
            _unlock_fd(fd)
            try:
                os.close(fd)
            except OSError:
                pass

    def __enter__(self) -> 'WorkerIdentityLease':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.release()

    def __del__(self) -> None:
        try:
            self.release()
        except Exception:
            pass
