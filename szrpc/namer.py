import itertools
import random
import copy
from typing import Generator

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
    def __init__(self, separator: str = '-', seed: int = None):
        random.seed(seed)
        self.separator = separator
        self.names = NAMES[:]
        self.adjectives = ADJECTIVES[:]
        self.total_names = len(self.names) * len(self.adjectives)
        random.shuffle(self.names)
        random.shuffle(self.adjectives)
        self.name_generator = self.fisher_yates(self.total_names)

    @staticmethod
    def fisher_yates(size: int) -> Generator[int, None, None]:
        """
        A generator that yields unique random integers from 0 to size-1.
        
        This implements a sparse Fisher-Yates shuffle, allowing for O(k) memory usage
        to generate k items, rather than O(size).
        """
        # Track swapped elements: index -> value
        # If an index is missing, its value is the index itself.
        swaps = {}
        
        for i in reversed(range(size)):
            j = random.randrange(i + 1)
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
            self.name_generator = self.fisher_yates(self.total_names)
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
