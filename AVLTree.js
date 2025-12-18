class Node {
    constructor(id) {
        this.id = id;
        this.left = null;
        this.right = null;
        this.height = 1;
    }
}

class IndexTree {
    constructor() {
        this.root = null;
    }

    getHeight(node) {
        return node ? node.height : 0;
    }

    getBalanceFactor(node) {
        if (!node) return 0;
        return this.getHeight(node.left) - this.getHeight(node.right);
    }

    rotateRight(y) {
        const x = y.left;
        const T2 = x.right;
        x.right = y;
        y.left = T2;
        y.height = Math.max(this.getHeight(y.left), this.getHeight(y.right)) + 1;
        x.height = Math.max(this.getHeight(x.left), this.getHeight(x.right)) + 1;
        return x;
    }

    rotateLeft(x) {
        const y = x.right;
        const T2 = y.left;
        y.left = x;
        x.right = T2;
        x.height = Math.max(this.getHeight(x.left), this.getHeight(x.right)) + 1;
        y.height = Math.max(this.getHeight(y.left), this.getHeight(y.right)) + 1;
        return y;
    }

    // Yeh hai asli function jo user ko call karna chahiye
    insert(id) {
        this.root = this._insertNode(this.root, id);
    }

    _insertNode(node, id) {
        if (node === null) {
            return new Node(id);
        }

        if (id < node.id) {
            node.left = this._insertNode(node.left, id);
        } else if (id > node.id) {
            node.right = this._insertNode(node.right, id);
        } else {
            return node; // Duplicate ID pe kuch nahi karega
        }

        node.height = 1 + Math.max(this.getHeight(node.left), this.getHeight(node.right));
        const balance = this.getBalanceFactor(node);

        if (balance > 1 && id < node.left.id) return this.rotateRight(node);
        if (balance < -1 && id > node.right.id) return this.rotateLeft(node);
        if (balance > 1 && id > node.left.id) {
            node.left = this.rotateLeft(node.left);
            return this.rotateRight(node);
        }
        if (balance < -1 && id < node.right.id) {
            node.right = this.rotateRight(node.right);
            return this.rotateLeft(node);
        }

        return node;
    }
}

const db = new IndexTree();
db.insert(10);
db.insert(20);
 db.insert(30);
 db.insert(40);
 db.insert(50);
 db.insert(25);

console.log(db.root.id);