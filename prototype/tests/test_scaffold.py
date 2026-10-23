def test_prototype_modules_are_importable():
    import prototype.actors
    import prototype.gateway

    assert prototype.actors.__file__
    assert prototype.gateway.__file__
