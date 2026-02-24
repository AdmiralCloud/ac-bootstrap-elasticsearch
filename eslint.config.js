const globals = require('globals')

module.exports = [
  {
    files: ['index.js', 'test/test.js'],
    languageOptions: {
      ecmaVersion: 2022,
      sourceType: 'module',
      globals: {
        ...globals.commonjs,
        ...globals.es2015,
        ...globals.node,
        expect: 'readonly',
        describe: 'readonly',
        it: 'readonly'
      }
    },
    rules: {
      'no-const-assign': 'error',
      'space-before-function-paren': 'off',
      'no-extra-semi': 'off',
      'object-curly-spacing': ['error', 'always'],
      'brace-style': ['error', 'stroustrup', { allowSingleLine: true }],
      'block-spacing': 'error',
      'no-useless-escape': 'off',
      'no-console': ['warn', { allow: ['warn', 'error'] }],
      'no-unused-vars': 'error',
      'eqeqeq': 'error',
      'no-var': 'error',
      'curly': 'error',
      'prefer-const': ['error', { ignoreReadBeforeAssign: true }]
    }
  }
]