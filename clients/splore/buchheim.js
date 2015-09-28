'use strict';

// JavaScript implementation of Christoph Buchheim, Michael Jünger, Sebastian Leipert's tree layout algorithm. See: http://dl.acm.org/citation.cfm?id=729576.
//
// Thanks also to Bill Mill for the explanation and Python sample code: http://billmill.org/pymag-trees/.

// TreeNode represents one node of the tree visualization.
function TreeNode(data, id, parent, depth, number, seen) {
	seen[id] = true;
	this.x = -1;
	this.y = depth;
	this.data = data.nodes[id];
	this.id = id;
	this.children = ((this.data.isOpen && data.links[id]) || [])
		.filter(cid => !(cid in seen))
		.map((cid, i) => new TreeNode(data, cid, this, depth+1, i+1, seen));
	this.parent = parent;
	this.thread = null;
	this.offset = 0;
	this.ancestor = this;
	this.change = 0;
	this.shift = 0;
	this.number = number;
	this.mod = 0;
}

TreeNode.prototype.left = function() {
	if (this.children.length > 0) {
		return this.children[0];
	} else {
		return this.thread;
	}
};

TreeNode.prototype.right = function() {
	if (this.children.length > 0) {
		return this.children[this.children.length - 1];
	} else {
		return this.thread;
	}
};

TreeNode.prototype.leftBrother = function() {
	var n = null;
	if (this.parent) {
		for (var node of this.parent.children) {
			if (node == this) {
				return n;
			} else {
				n = node;
			}
		}
	}
	return n;
};

TreeNode.prototype.getLeftMostSibling = function() {
	if (this.parent && this != this.parent.children[0]) {
		return this.parent.children[0];
	} else {
		return null;
	}
};

function layout(tree) {
	firstWalk(tree, 1);
	secondWalk(tree, 0, 0);
}

function firstWalk(v, distance) {
	if (v.children.length == 0) {
		if (v.getLeftMostSibling()) {
			v.x = v.leftBrother().x + distance;
		} else {
			v.x = 0;
		}
	} else {
		var defaultAncestor = v.children[0];
		for (var w of v.children) {
			firstWalk(w, distance);
			defaultAncestor = apportion(w, defaultAncestor, distance);
		}
		executeShifts(v);

		var midpoint = (v.children[0].x + v.children[v.children.length - 1].x) / 2;

		var ell = v.children[0];
		var arr = v.children[v.children.length - 1];
		var w = v.leftBrother();
		if (w) {
			v.x = w.x + distance;
			v.mod = v.x - midpoint;
		} else {
			v.x = midpoint;
		}
	}

	return v;
}

function apportion(v, defaultAncestor, distance) {
    var w = v.leftBrother();
    if (w != null) {
        var vir = v;
        var vor = v;
        var vil = w;
        var vol = v.getLeftMostSibling();
        var sir = v.mod;
        var sor = v.mod;
        var sil = vil.mod;
        var sol = vol.mod;
        while (vil.right() && vir.left()) {
            vil = vil.right();
            vir = vir.left();
            vol = vol.left();
            vor = vor.right();
            vor.ancestor = v
            var shift = (vil.x + sil) - (vir.x + sir) + distance;
            if (shift > 0) {
                var a = ancestor(vil, v, defaultAncestor);
                moveSubtree(a, v, shift);
                sir = sir + shift;
                sor = sor + shift;
            }
            sil += vil.mod;
            sir += vir.mod;
            sol += vol.mod;
            sor += vor.mod;
        }
        if (vil.right() && !vor.right()) {
            vor.thread = vil.right();
            vor.mod += sil - sor;
        } else {
        	// In the original algorithm the else above is not there.
            if (vir.left() && !vol.left()) {
                vol.thread = vir.left();
                vol.mod += sir - sol;
            }
            defaultAncestor = v;
		}
	}
    return defaultAncestor
 }

function moveSubtree(wl, wr, shift) {
    var subtrees = wr.number - wl.number;
    wr.change -= shift / subtrees;
    wr.shift += shift;
    wl.change += shift / subtrees;
    wr.x += shift;
    wr.mod += shift;
}

function executeShifts(v) {
    var shift = 0;
    var change = 0;
    for (var i = v.children.length - 1; i >= 0; i--) {
    	var w = v.children[i];
        w.x += shift;
        w.mod += shift;
        change += w.change;
        shift += w.shift + change;
    }
}

function ancestor(vil, v, defaultAncestor) {
    if (v.parent.children.indexOf(vil.ancestor) > -1) {
        return vil.ancestor;
    } else {
        return defaultAncestor;
    }
}

function secondWalk(v, m, depth) {
    v.x += m
    v.y = depth

    for (var w of v.children) {
        secondWalk(w, m + v.mod, depth+1)
    }
}

module.exports = {TreeNode: TreeNode, layout: layout};
