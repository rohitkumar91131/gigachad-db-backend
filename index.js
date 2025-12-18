const fs = require('fs');
const express = require('express');
const app = express();
const cors = require("cors");
const DB_FILENAME = 'users.jsonl';
const IDX_FILENAME = 'users.idx';

class Node {
  constructor(id, filePosition) {
    this.id = id;
    this.left = null;
    this.right = null;
    this.height = 1;
    this.filePosition = filePosition;
  }
}

class AvlIndexTree {
  constructor() {
    this.root = null;
  }

  getHeight(n) {
    return n ? n.height : 0;
  }

  getBalance(n) {
    return n
      ? this.getHeight(n.left) - this.getHeight(n.right)
      : 0;
  }

  rotateRight(y) {
    const x = y.left;
    const T2 = x.right;

    x.right = y;
    y.left = T2;

    y.height =
      Math.max(
        this.getHeight(y.left),
        this.getHeight(y.right)
      ) + 1;

    x.height =
      Math.max(
        this.getHeight(x.left),
        this.getHeight(x.right)
      ) + 1;

    return x;
  }

  rotateLeft(x) {
    const y = x.right;
    const T2 = y.left;

    y.left = x;
    x.right = T2;

    x.height =
      Math.max(
        this.getHeight(x.left),
        this.getHeight(x.right)
      ) + 1;

    y.height =
      Math.max(
        this.getHeight(y.left),
        this.getHeight(y.right)
      ) + 1;

    return y;
  }

  insert(id, filePosition) {
    this.root = this._insert(
      this.root,
      id,
      filePosition
    );
  }

  _insert(node, id, filePosition) {
    if (!node) {
      //console.log("➕ Insert", id, "at", filePosition);
      return new Node(id, filePosition);
    }

    if (id < node.id) {
      node.left = this._insert(
        node.left,
        id,
        filePosition
      );
    } else if (id > node.id) {
      node.right = this._insert(
        node.right,
        id,
        filePosition
      );
    } else {
      //console.log("♻️ Update index", id);
      node.filePosition = filePosition;
      return node;
    }

    node.height =
      1 +
      Math.max(
        this.getHeight(node.left),
        this.getHeight(node.right)
      );

    const balance = this.getBalance(node);

    if (balance > 1 && id < node.left.id) {
      //console.log("↩️ Right Rotate", node.id);
      return this.rotateRight(node);
    }

    if (balance < -1 && id > node.right.id) {
      //onsole.log("↪️ Left Rotate", node.id);
      return this.rotateLeft(node);
    }

    if (balance > 1 && id > node.left.id) {
      //console.log("🔁 Left-Right Rotate", node.id);
      node.left = this.rotateLeft(node.left);
      return this.rotateRight(node);
    }

    if (balance < -1 && id < node.right.id) {
      //console.log("🔁 Right-Left Rotate", node.id);
      node.right = this.rotateRight(node.right);
      return this.rotateLeft(node);
    }

    return node;
  }

  findFilePosition(id) {
    let current = this.root;

    while (current) {
      if (id === current.id) {
        return current.filePosition;
      }
      if (id < current.id) {
        current = current.left;
      } else {
        current = current.right;
      }
    }
    return null;
  }

  toArray() {
    console.time("Index serialization time");

    const res = [];
    const stack = [];
    let cur = this.root;

    while (cur || stack.length) {
      while (cur) {
        stack.push(cur);
        cur = cur.left;
      }
      cur = stack.pop();
      res.push({
        id: cur.id,
        filePosition: cur.filePosition
      });
      cur = cur.right;
    }

    console.timeEnd("Index serialization time");
    return res;
  }

  toTree(list) {
    console.time("Index load time");
    this.root = null;

    for (const n of list) {
      this.insert(n.id, n.filePosition);
    }

    console.timeEnd("Index load time");
  }
}

class GigaDb {
  constructor() {
    this.indexTree = new AvlIndexTree();
  }

  init() {
    if (!fs.existsSync(DB_FILENAME)) {
      fs.writeFileSync(DB_FILENAME, '');
      console.log("📁 DB file created");
      this.seed(100000);
    }

    if (fs.existsSync(IDX_FILENAME)) {
      //console.log("⚡ Fast boot: loading index");
      const raw = fs.readFileSync(
        IDX_FILENAME,
        'utf-8'
      );
      this.indexTree.toTree(JSON.parse(raw));
    } else {
      console.log("🛠️ Rebuilding index");
      this.rebuildIndex();
      this.saveIndex();
    }
  }

  async seed(count) {
    console.time("Seeding time");

    const stream = fs.createWriteStream(
      DB_FILENAME,
      { flags: 'a' }
    );

    for (let i = 0; i < count; i++) {
      const user = {
        id: i + 1,
        name: `User${i}`,
        email: `user${i}@gmail.com`
      };

      const data =
        JSON.stringify(user) + '\n';

      const len = Buffer.alloc(4);
      len.writeUInt32BE(data.length);

      const buf = Buffer.concat([
        len,
        Buffer.from(data)
      ]);

      if (!stream.write(buf)) {
        await new Promise(r =>
          stream.once("drain", r)
        );
      }

      console.log("Inserted user", user.id);
    }

    stream.end();
    console.timeEnd("Seeding time");
  }

  rebuildIndex() {
    const data = fs.readFileSync(
      DB_FILENAME,
      'utf-8'
    );

    const lines = data.split('\n');
    let pos = 0;

    for (const line of lines) {
      if (!line) continue;

      const match = line.match(/"id":(\d+)/);
      if (match) {
        console.log(
          "Indexing ID",
          match[1],
          "at",
          pos
        );
        this.indexTree.insert(
          parseInt(match[1]),
          pos
        );
      }

      pos += Buffer.byteLength(line + '\n');
    }
  }

  saveIndex() {
    console.log("💾 Saving index");
    fs.writeFileSync(
      IDX_FILENAME,
      JSON.stringify(this.indexTree.toArray())
    );
  }

  findById(id) {
    const start = process.hrtime.bigint();

    id = parseInt(id);

    const position =
      this.indexTree.findFilePosition(id);

    if (position === null) {
      const end = process.hrtime.bigint();
      return {
        data: null,
        time_ms: Number(end - start) / 1e6
      };
    }

    const fd = fs.openSync(DB_FILENAME, "r");

    const lenBuf = Buffer.alloc(4);
    fs.readSync(fd, lenBuf, 0, 4, position);

    const size = lenBuf.readUInt32BE(0);

    const dataBuf = Buffer.alloc(size);
    fs.readSync(
      fd,
      dataBuf,
      0,
      size,
      position + 4
    );

    fs.closeSync(fd);
    

    const end = process.hrtime.bigint();

    return {
      data: JSON.parse(
        dataBuf.toString("utf8")
      ),
      time_ms: Number(end - start) / 1e6
    };
  }

  findByPage(pageNumeber){
    const start = process.hrtime.bigint();
    pageNumeber = parseInt(pageNumeber);

    let lastUser ;
    let firstUser ;
    if(pageNumeber === 1){
        firstUser = 1;
        lastUser = 99;
    }
    else if (pageNumeber < 1) return null;
    else{
        lastUser = pageNumeber*100;
        firstUser = lastUser-100;
    }

    const fd = fs.openSync(DB_FILENAME , 'r');
    const users = [];

    for(let i = firstUser ; i < lastUser ; i++ ){
        const position = this.indexTree.findFilePosition(i);
        if(!position) {
            continue;
        }

        const lenbuf = Buffer.alloc(4);
        fs.readSync(fd , lenbuf , 0 , 4 , position);
        const size = lenbuf.readInt32BE(0);
        const dataBuffer = Buffer.alloc(size);

        fs.readSync(fd,dataBuffer , 0 , size , position +4);

        users.push(JSON.parse(dataBuffer));
    }

    fs.closeSync(fd);

    const end = process.hrtime.bigint();

    return {
        users : users,
        time_ms: Number(end - start) / 1e6
    }

  }
}

const db = new GigaDb();
db.init();
db.findById(1000);




app.use(cors());
app.use(express.json());

app.get("/:id", (req, res) => {
  try {
    const id = parseInt(req.params.id);

    if (Number.isNaN(id)) {
      return res.status(400).json({
        success: false,
        msg: "Valid id required"
      });
    }

    const result = db.findById(id);

    if (!result.data) {
      return res.status(404).json({
        success: false,
        time_ms: result.time_ms,
        msg: "User not found"
      });
    }

    res.json({
      success: true,
      time_ms: result.time_ms,
      user: result.data
    });

  } catch (err) {
    res.status(500).json({
      success: false,
      msg: err.message
    });
  }
});

app.get("/users/page/:id", (req, res) => {
  try {
    const page = req.params.id;
    const result = db.findByPage(page);
    //console.log(result.time_ms)

    res.json({
      success: true,
      page: Number(page),
      users: result.users,
      time_taken: result.time_ms
    });
  } catch (err) {
    res.status(500).json({
      success: false,
      msg: err.message
    });
  }
});

app.listen(7101, () => {
  console.log(
    "🚀 DB Server running on port 7101"
  );
});
