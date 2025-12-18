class Node{
    constructor(value){
        this.value = value
        this.left = null
        this.right = null
        this.height = 0
    }
}

class BinarySearchTree{
    constructor(){
        this.root = null;
    }

    insert(value){
        const newNode = new Node(value);

        if(this.root === null){
            this.root = newNode;
            return;
        }

        let current = this.root;

        while(true){
            if(value === current.value) return; 
            if(value < current.value){
                if(current.left === null){
                    current.left = newNode;
                    return;
                }

                current = current.left;
            }

            if(value > current.value){
                if(current.right === null){
                    current.right = newNode;
                    return;
                }

                current = current.right;    
            }
        }

         
    }
}



Binary search tree me baar baar loop chalta jisse left aur right child pe jata hai jab tak ki sahi jagah na mil jaye naya node insert karne ke liye. Agar value current node ke value se chhoti hai to left child pe jata hai, aur agar badi hai to right child pe jata hai. Jab ek null position milti hai, tab naya node wahan insert kar diya jata hai.
